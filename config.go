package queue

import (
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// Config describes the Valkey connection shared by the queue client, worker,
// scheduler and outbox relay. Compose it under a prefix:
//
//	type Config struct {
//		Valkey queue.Config `envPrefix:"VALKEY_"`
//	}
//
// which maps to VALKEY_ADDR, VALKEY_PASSWORD, VALKEY_DB.
type Config struct {
	// Addr is the Valkey host:port.
	Addr string `env:"ADDR" envDefault:"localhost:6379"`
	// Password authenticates the connection; empty for no auth.
	Password string `env:"PASSWORD"`
	// DB selects the Valkey logical database.
	DB int `env:"DB" envDefault:"0"`
}

// ValkeyConnOpt renders the config as asynq's connection option, used to
// build the client, server and scheduler.
func (c Config) ValkeyConnOpt() asynq.RedisClientOpt {
	return asynq.RedisClientOpt{
		Addr:     c.Addr,
		Password: c.Password,
		DB:       c.DB,
	}
}

// ValkeyOptions renders the config as go-redis connection options (Valkey is
// RESP/command compatible with Redis), the single source of truth for every
// direct redis.NewClient call in the kit (queue, scheduler, ...).
func (c Config) ValkeyOptions() *redis.Options {
	return &redis.Options{
		Addr:     c.Addr,
		Password: c.Password,
		DB:       c.DB,
	}
}
