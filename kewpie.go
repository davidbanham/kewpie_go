package kewpie

import (
	"context"
	"fmt"
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

type Backend interface {
	Publish(ctx context.Context, queueName string, payload *types.Task) error
	Subscribe(ctx context.Context, queueName string, handler types.Handler) error
	Pop(ctx context.Context, queueName string, handler types.Handler) error
	Init(queues []string) error
	Disconnect() error
	Purge(ctx context.Context, queueName string) error
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
	err = this.backend.Subscribe(ctx, queueName, handler)
	return
}

func (this Kewpie) Pop(ctx context.Context, queueName string, handler types.Handler) error {
	return this.backend.Pop(ctx, queueName, handler)
}

func (this *Kewpie) Connect(backend string, queues []string) (err error) {
	this.queues = queues

	switch backend {
	case "memory":
		this.backend = &memory.MemoryStore{}
	case "sqs":
		this.backend = &sqs.Sqs{}
	case "postgres":
		this.backend = &postgres.Postgres{}
	default:
		return fmt.Errorf("Unknown backend")
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
