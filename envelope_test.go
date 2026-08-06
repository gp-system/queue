package queue_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/gp-system/queue"
)

func TestTaskTypeHelpers(t *testing.T) {
	cases := []struct {
		got  string
		want string
	}{
		{queue.EventTaskType("news.published"), "event:news.published"},
		{queue.ListenerTaskType("news.published", "email"), "listener:news.published:email"},
		{queue.JobTaskType("cleanup"), "job:cleanup"},
		{queue.ScheduleTaskType("news.published"), "schedule:news.published"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("got %q, want %q", c.got, c.want)
		}
	}

	if !queue.IsEventTaskType("event:x") || queue.IsEventTaskType("listener:x") {
		t.Error("IsEventTaskType")
	}
	if !queue.IsListenerTaskType("listener:x:y") || queue.IsListenerTaskType("event:x") {
		t.Error("IsListenerTaskType")
	}
	if !queue.IsJobTaskType("job:x") || queue.IsJobTaskType("event:x") {
		t.Error("IsJobTaskType")
	}
	if !queue.IsScheduleTaskType("schedule:x") || queue.IsScheduleTaskType("event:x") {
		t.Error("IsScheduleTaskType")
	}
}

func TestEnvelopeRoundTrip(t *testing.T) {
	env := queue.Envelope{
		ID:         "id-1",
		Name:       "news.published",
		Payload:    json.RawMessage(`{"title":"hi"}`),
		Metadata:   map[string]string{"k": "v"},
		OccurredAt: time.Now().UTC().Truncate(time.Second),
	}
	task, err := env.Task()
	if err != nil {
		t.Fatalf("Task: %v", err)
	}
	if task.Type() != "event:news.published" {
		t.Fatalf("task type %q", task.Type())
	}

	got, err := queue.DecodeEnvelope(task)
	if err != nil {
		t.Fatalf("DecodeEnvelope: %v", err)
	}
	if got.ID != env.ID || got.Name != env.Name || got.Metadata["k"] != "v" {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if string(got.Payload) != string(env.Payload) {
		t.Fatalf("payload mismatch: %s", got.Payload)
	}
	if !got.OccurredAt.Equal(env.OccurredAt) {
		t.Fatalf("time mismatch: %v vs %v", got.OccurredAt, env.OccurredAt)
	}
}

func TestEnvelopeTracePropagation(t *testing.T) {
	otel.SetTextMapPropagator(propagation.TraceContext{})

	traceID, _ := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	spanID, _ := trace.SpanIDFromHex("0123456789abcdef")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	var env queue.Envelope
	env.InjectTrace(ctx)
	if env.Metadata["traceparent"] == "" {
		t.Fatalf("traceparent not injected: %+v", env.Metadata)
	}

	got := trace.SpanContextFromContext(env.ExtractTrace(context.Background()))
	if got.TraceID() != traceID {
		t.Fatalf("trace id not propagated: got %s", got.TraceID())
	}
}
