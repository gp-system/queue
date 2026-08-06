# queue

A Valkey/[`asynq`](https://github.com/hibiken/asynq) foundation for
background-job systems: connection config, a task client verified with a PING
at construction, a common enqueue-option subset that covers most use cases,
and an event `Envelope` wire format (with OTel trace propagation).

`queue` has no dependency beyond
[`github.com/gp-system/errs`](https://github.com/gp-system/errs), so it works
in any Go program built on `asynq` and `go-redis` (Valkey is RESP/command
compatible with Redis, so the go-redis client works against it unchanged).
It is also the queue foundation used by the
[gp-system](https://github.com/gp-system) backend kit's events, scheduler,
outbox and worker layers.

## The problem it solves

A background-job system built on asynq needs a few decisions made once and
shared everywhere: what the Valkey connection config looks like, what a task
type string means (is it an event, a listener, a scheduled job?), how a task
payload carries a trace context across the enqueue/process hop, and what
"duplicate" means when idempotent delivery is required. `queue` centralizes
those decisions once, so the different layers of a background-job system
(event dispatch, scheduling, outbox relay, worker processing) and
application code build on the same primitives instead of reinventing them
per project. asynq types (`*asynq.Task`, `*asynq.TaskInfo`) are not hidden:
application code that needs the full asynq API is free to use it directly
alongside this package.

## Install

```sh
go get github.com/gp-system/queue
```

## Usage

### Config and Client

`Config` describes the shared Valkey connection; compose it under a prefix:

```go
type Config struct {
	Valkey queue.Config `envPrefix:"VALKEY_"`
}
```

which maps to `VALKEY_ADDR`, `VALKEY_PASSWORD`, `VALKEY_DB`. `NewClient` opens
the connection, pings it, and builds an asynq client:

```go
client, err := queue.NewClient(ctx, cfg.Valkey)
if err != nil {
	log.Fatal(err)
}
defer client.Close()

info, err := client.Enqueue(ctx, task, queue.OnQueue("emails"), queue.MaxRetry(3))
if errors.Is(err, queue.ErrDuplicate) {
	// a TaskID/Unique constraint suppressed the enqueue; treat as success
}
```

`MustNewClient` is `NewClient` but panics on error, for use in `main()`.

### Options

`Option` re-exports the useful subset of asynq's enqueue options so common
cases don't require importing asynq directly: `OnQueue`, `MaxRetry`,
`Timeout`, `Deadline`, `ProcessIn`, `Unique`, `Retention`, `TaskID`.
`AsynqOptions` converts them to raw `[]asynq.Option` for code that calls
asynq directly (e.g. a scheduler registrar) rather than through
`Client.Enqueue`.

### Task type naming

Every task type is one of four prefixed shapes, so a single `asynq.ServeMux`
can classify any task type it sees without a side channel:

| Prefix | Constructor | Meaning |
|---|---|---|
| `event:` | `EventTaskType(name)` | an event's fan-out task |
| `listener:` | `ListenerTaskType(event, listener)` | one listener's copy of an event |
| `job:` | `JobTaskType(name)` | a scheduled job |
| `schedule:` | `ScheduleTaskType(name)` | a scheduled event trigger |

`IsEventTaskType`, `IsListenerTaskType`, `IsJobTaskType`, `IsScheduleTaskType`
classify a task type string; `JobName` extracts the job name back out of a
job task type.

### Envelope

`Envelope` is the wire format of every event task payload: it travels from
the dispatcher (or outbox) through the fan-out task to each listener task,
carrying the same id, payload and trace context throughout.

```go
env := queue.Envelope{
	ID:         id,
	Name:       "news.published",
	Payload:    payload,
	OccurredAt: time.Now().UTC(),
}
env.InjectTrace(ctx)

task, err := env.Task()
```

`DecodeEnvelope` extracts the envelope back out of a task on the consuming
side; `ExtractTrace` returns a context carrying the trace context stored in
the envelope's metadata, so spans link across the dispatch, relay and worker
hop.

## Design rules

- **Centralize the wire format, don't hide asynq.** The task-type prefixes
  and envelope shape are the single source of truth every layer (and
  application code) reads and writes against; asynq itself is never wrapped
  away.
- **Duplicates are a sentinel, not the raw asynq error.** `Enqueue` maps
  `asynq.ErrTaskIDConflict`/`asynq.ErrDuplicateTask` to `ErrDuplicate`.
  Wrapping a sentinel error is idiomatic Go, not a package-specific
  convention: callers check it with `errors.Is` instead of matching the
  underlying asynq error type directly.
- **The client pings at construction.** A broken Valkey connection fails at
  startup, not on the first enqueue, matching `pg.NewPool`'s convention.
