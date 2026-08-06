package queue

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/hibiken/asynq"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/gp-system/errs"
)

// Task type prefixes. Event dispatch enqueues an EventTaskType; the worker's
// fan-out handler expands it into one ListenerTaskType per registered listener.
// Scheduled jobs use JobTaskType. Exported so a asynq.ServeMux is registered
// against the same constants this package uses to classify task types —
// there must be exactly one source of truth for the wire prefixes.
const (
	EventTaskPrefix    = "event:"
	ListenerTaskPrefix = "listener:"
	JobTaskPrefix      = "job:"
	ScheduleTaskPrefix = "schedule:"
)

// EventTaskType is the asynq task type for an event's fan-out task.
func EventTaskType(name string) string { return EventTaskPrefix + name }

// ListenerTaskType is the asynq task type for one listener of an event.
func ListenerTaskType(event, listener string) string {
	return ListenerTaskPrefix + event + ":" + listener
}

// JobTaskType is the asynq task type for a scheduled job.
func JobTaskType(name string) string { return JobTaskPrefix + name }

// IsJobTaskType reports whether a task type is a scheduled job.
func IsJobTaskType(t string) bool { return has(t, JobTaskPrefix) }

// JobName returns the job name encoded in a job task type, and false if t is
// not a job task type.
func JobName(t string) (string, bool) {
	if !IsJobTaskType(t) {
		return "", false
	}
	return t[len(JobTaskPrefix):], true
}

// ScheduleTaskType is the asynq task type for a scheduled event trigger. The
// worker rebuilds a fresh envelope per fire (keyed on the trigger's task id)
// and fans it out, so each scheduled fire delivers to every listener once.
func ScheduleTaskType(name string) string { return ScheduleTaskPrefix + name }

// IsEventTaskType reports whether a task type is an event fan-out task.
func IsEventTaskType(t string) bool { return has(t, EventTaskPrefix) }

// IsListenerTaskType reports whether a task type is a listener task.
func IsListenerTaskType(t string) bool { return has(t, ListenerTaskPrefix) }

// IsScheduleTaskType reports whether a task type is a scheduled event trigger.
func IsScheduleTaskType(t string) bool { return has(t, ScheduleTaskPrefix) }

func has(s, prefix string) bool {
	return strings.HasPrefix(s, prefix)
}

// Envelope is the wire format of every event task payload. It travels from the
// dispatcher (or outbox) through the fan-out task to each listener task, so the
// listener sees the same id, payload and trace context the producer set.
type Envelope struct {
	// ID is a unique identifier for this dispatch (also used as the asynq
	// TaskID for deduplication). Listeners use it as an idempotency key.
	ID string `json:"id"`
	// Name is the event name (Event.EventName()).
	Name string `json:"name"`
	// Payload is the JSON-encoded event value.
	Payload json.RawMessage `json:"payload"`
	// Metadata carries the W3C trace context (traceparent/tracestate) so spans
	// link across the dispatch → relay → worker hop.
	Metadata map[string]string `json:"metadata,omitempty"`
	// OccurredAt is when the event was dispatched.
	OccurredAt time.Time `json:"occurred_at"`
}

// Task renders the envelope as the event fan-out task.
func (e *Envelope) Task() (*asynq.Task, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return nil, errs.Wrap(err, "queue: marshal envelope", errs.With("event", e.Name))
	}
	return asynq.NewTask(EventTaskType(e.Name), data), nil
}

// DecodeEnvelope extracts the envelope from a task payload.
func DecodeEnvelope(t *asynq.Task) (Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(t.Payload(), &e); err != nil {
		return Envelope{}, errs.Wrap(err, "queue: decode envelope", errs.With("task_type", t.Type()))
	}
	return e, nil
}

// InjectTrace writes the trace context carried by ctx into the envelope
// metadata, so downstream consumers can continue the trace.
func (e *Envelope) InjectTrace(ctx context.Context) {
	carrier := propagation.MapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	if len(carrier) > 0 {
		e.Metadata = carrier
	}
}

// ExtractTrace returns a context carrying the trace context stored in the
// envelope metadata (a no-op when none was injected).
func (e *Envelope) ExtractTrace(ctx context.Context) context.Context {
	if len(e.Metadata) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier(e.Metadata))
}
