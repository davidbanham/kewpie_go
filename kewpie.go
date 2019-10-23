package kewpie

import (
	"context"
	"database/sql"
	"time"

	"github.com/davidbanham/kewpie_go/backends/memory"
	"github.com/davidbanham/kewpie_go/backends/postgres"
	"github.com/davidbanham/kewpie_go/backends/sqs"
	"github.com/davidbanham/kewpie_go/types"
)

type Kewpie struct {
	queues  []string
	backend Backend
}

type Task = types.Task
type Tags = types.Tags

type Backend interface {
	Healthy(ctx context.Context) error
	Publish(ctx context.Context, queueName string, payload *types.Task) error
	Subscribe(ctx context.Context, queueName string, handler types.Handler) error
	Pop(ctx context.Context, queueName string, handler types.Handler) error
	Init(queues []string) error
	Disconnect() error
	Purge(ctx context.Context, queueName string) error
	PurgeMatching(ctx context.Context, queueName, substr string) error
}

func (this Kewpie) Publish(ctx context.Context, queueName string, payload *types.Task) (err error) {
	// Set RunAt based on the info from Delay
	if payload.Delay != 0 {
		payload.RunAt = time.Now().Add(payload.Delay)
	} else if payload.RunAt.IsZero() {
		payload.RunAt = time.Now()
	}

	payload.Delay = payload.RunAt.Sub(time.Now())

	err = this.backend.Publish(ctx, queueName, payload)

	return
}

func (this Kewpie) Subscribe(ctx context.Context, queueName string, handler types.Handler) (err error) {
	return this.backend.Subscribe(ctx, queueName, handler)
}

func (this Kewpie) Pop(ctx context.Context, queueName string, handler types.Handler) error {
	return this.backend.Pop(ctx, queueName, handler)
}

func (this *Kewpie) Connect(backend string, queues []string, connection interface{}) (err error) {
	this.queues = queues

	switch backend {
	case "memory":
		this.backend = &memory.MemoryStore{}
	case "sqs":
		this.backend = &sqs.Sqs{}
	case "postgres":
		pgbe := &postgres.Postgres{}
		pgbe.PassConnection(connection.(*sql.DB))
		this.backend = pgbe
	default:
		return types.UnknownBackend
	}

	err = this.backend.Init(queues)
	return
}

func (this Kewpie) Disconnect() error {
	return this.backend.Disconnect()
}

func (this Kewpie) Purge(ctx context.Context, queueName string) error {
	return this.backend.Purge(ctx, queueName)
}

func (this Kewpie) PurgeMatching(ctx context.Context, queueName, substr string) error {
	return this.backend.PurgeMatching(ctx, queueName, substr)
}

func (this Kewpie) Healthy(ctx context.Context) error {
	return this.backend.Healthy(ctx)
}
