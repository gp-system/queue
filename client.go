// Package queue is the kit's Valkey/asynq foundation: connection config, a task
// client verified with a PING at construction, the enqueue-option subset the
// rest of the kit uses, and the event Envelope wire format (with OTel trace
// propagation). The events, scheduler, outbox and worker packages build on it.
// The point is centralizing this core once — task-type prefixes, envelope
// shape, trace propagation — so it isn't reinvented per project; asynq types
// (e.g. *asynq.Task, *asynq.TaskInfo) are not hidden and application code may
// use this package and asynq directly when events/scheduler don't fit.
package queue

import (
	"context"
	"errors"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/gp-system/errs"
)

var tracer = otel.Tracer("github.com/gp-system/queue")

// ErrDuplicate is returned by Enqueue when a TaskID or Unique constraint
// suppressed the enqueue because an identical task already exists. Callers that
// rely on idempotent delivery (the outbox relay, event fan-out) treat it as
// success.
var ErrDuplicate = errors.New("queue: duplicate task")

// Client wraps an asynq.Client over one shared go-redis connection to
// Valkey. The connection is verified with a PING at construction, matching
// the kit convention (pg.NewPool pings the pool).
type Client struct {
	rdb redis.UniversalClient
	ac  *asynq.Client
}

// NewClient opens the Valkey connection, pings it, and builds an asynq client.
func NewClient(ctx context.Context, cfg Config) (*Client, error) {
	rdb := redis.NewClient(cfg.ValkeyOptions())
	if err := rdb.Ping(ctx).Err(); err != nil {
		_ = rdb.Close()
		return nil, errs.Wrap(err, "queue: ping", errs.With("addr", cfg.Addr))
	}
	return &Client{rdb: rdb, ac: asynq.NewClientFromRedisClient(rdb)}, nil
}

// MustNewClient is NewClient but panics on error. Intended for main().
func MustNewClient(ctx context.Context, cfg Config) *Client {
	c, err := NewClient(ctx, cfg)
	if err != nil {
		panic(err)
	}
	return c
}

// Enqueue submits a task to Valkey. It returns ErrDuplicate (per the kit
// convention of wrapping a sentinel, not the underlying asynq error) when a
// TaskID/Unique constraint suppressed the enqueue.
func (c *Client) Enqueue(ctx context.Context, task *asynq.Task, opts ...Option) (*asynq.TaskInfo, error) {
	ctx, span := tracer.Start(ctx, "enqueue "+task.Type(), trace.WithSpanKind(trace.SpanKindProducer))
	defer span.End()
	span.SetAttributes(attribute.String("messaging.asynq.task_type", task.Type()))

	info, err := c.ac.EnqueueContext(ctx, task, buildOptions(opts).build()...)
	if err != nil {
		// A duplicate is expected idempotent behavior, not a failure, so it is
		// not recorded on the span.
		if errors.Is(err, asynq.ErrTaskIDConflict) || errors.Is(err, asynq.ErrDuplicateTask) {
			return nil, errs.Wrap(ErrDuplicate, "queue: enqueue", errs.With("task_type", task.Type()))
		}
		span.RecordError(err)
		return nil, errs.Wrap(err, "queue: enqueue", errs.With("task_type", task.Type()))
	}
	if info != nil {
		span.SetAttributes(attribute.String("messaging.message.id", info.ID))
	}
	return info, nil
}

// Close releases the underlying Valkey connection. The asynq client is built
// from this shared connection and does not own it (asynq refuses to close a
// shared client), so closing the connection is the complete teardown.
func (c *Client) Close() error {
	return errs.Wrap(c.rdb.Close(), "queue: close")
}
