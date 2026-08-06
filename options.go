package queue

import (
	"time"

	"github.com/hibiken/asynq"
)

// Option customizes how a task is enqueued. The kit re-exports the useful
// subset of asynq's enqueue options so common cases don't require importing
// asynq directly; application code that needs the full option set is free to
// use AsynqOptions/FromAsynqOptions and asynq types directly — the goal is a
// centralized, shared building block, not hiding asynq.
type Option func(*taskOptions)

type taskOptions struct {
	asynq []asynq.Option
}

func (o taskOptions) build() []asynq.Option { return o.asynq }

// AsynqOptions converts kit options to raw asynq options, for code that calls
// asynq directly (the scheduler registrar) rather than through Client.Enqueue.
func AsynqOptions(opts ...Option) []asynq.Option {
	return buildOptions(opts).build()
}

func buildOptions(opts []Option) taskOptions {
	var o taskOptions
	for _, opt := range opts {
		opt(&o)
	}
	return o
}

func add(opt asynq.Option) Option {
	return func(o *taskOptions) { o.asynq = append(o.asynq, opt) }
}

// OnQueue routes the task to a named queue (matched against the worker's
// WORKER_QUEUES weights). Unset means the "default" queue.
func OnQueue(name string) Option { return add(asynq.Queue(name)) }

// MaxRetry sets how many times a failed task is retried before it is archived.
func MaxRetry(n int) Option { return add(asynq.MaxRetry(n)) }

// Timeout bounds a single execution attempt.
func Timeout(d time.Duration) Option { return add(asynq.Timeout(d)) }

// Deadline sets an absolute deadline for the task across all attempts.
func Deadline(t time.Time) Option { return add(asynq.Deadline(t)) }

// ProcessIn delays processing by d from enqueue time (Laravel's ->delay()).
func ProcessIn(d time.Duration) Option { return add(asynq.ProcessIn(d)) }

// Unique suppresses enqueue of an identical task (same type + payload) while a
// previous one is still pending within ttl.
func Unique(ttl time.Duration) Option { return add(asynq.Unique(ttl)) }

// Retention keeps a task in Valkey for d after it completes, for inspection.
func Retention(d time.Duration) Option { return add(asynq.Retention(d)) }

// TaskID sets an explicit task ID; enqueuing a second task with the same ID
// while the first is still around is rejected (surfaced as ErrDuplicate). Used
// by the outbox relay and event fan-out for idempotent delivery.
func TaskID(id string) Option { return add(asynq.TaskID(id)) }
