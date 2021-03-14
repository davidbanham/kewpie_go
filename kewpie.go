package kewpie

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"

	"github.com/davidbanham/kewpie_go/v3/backends/googlepubsub"
	"github.com/davidbanham/kewpie_go/v3/backends/memory"
	"github.com/davidbanham/kewpie_go/v3/backends/postgres"
	"github.com/davidbanham/kewpie_go/v3/backends/sqs"
	"github.com/davidbanham/kewpie_go/v3/types"
	uuid "github.com/satori/go.uuid"
)

type Kewpie struct {
	queues   []string
	backend  Backend
	id       string
	bufferID string
}

type Task = types.Task
type Tags = types.Tags

type bufferedTask struct {
	QueueName string
	Task      *Task
}

type buffer []bufferedTask

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

func (this Kewpie) Publish(ctx context.Context, queueName string, payload *types.Task) error {
	// Set RunAt based on the info from Delay
	if payload.Delay != 0 {
		payload.RunAt = time.Now().Add(payload.Delay)
	} else if payload.RunAt.IsZero() {
		payload.RunAt = time.Now()
	}

	payload.Delay = payload.RunAt.Sub(time.Now())

	return this.backend.Publish(ctx, queueName, payload)
}

func (this Kewpie) Subscribe(ctx context.Context, queueName string, handler types.Handler) error {
	return this.backend.Subscribe(ctx, queueName, handler)
}

func (this Kewpie) Pop(ctx context.Context, queueName string, handler types.Handler) error {
	return this.backend.Pop(ctx, queueName, handler)
}

func (this *Kewpie) Connect(backend string, queues []string, connection interface{}) error {
	this.queues = queues
	this.id = uuid.NewV4().String()
	this.bufferID = "kewpie_buffer_" + this.id

	switch backend {
	case "memory":
		this.backend = &memory.MemoryStore{}
	case "sqs":
		this.backend = &sqs.Sqs{}
	case "postgres":
		pgbe := &postgres.Postgres{}

		switch v := connection.(type) {
		case nil:
			log.Println("No connection passed. PG backend will instantiate internally")
		case *sql.DB:
			pgbe.PassConnection(v)
		}
		this.backend = pgbe
	case "google_pubsub":
		this.backend = &googlepubsub.PubSub{}
	default:
		return types.UnknownBackend
	}

	return this.backend.Init(queues)
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

func (this Kewpie) PrepareContext(ctx context.Context) context.Context {
	if this.bufferID == "" {
		return ctx
	}
	buf := buffer{}
	return context.WithValue(ctx, this.bufferID, &buf)
}

func (this Kewpie) Buffer(ctx context.Context, queueName string, payload *Task) error {
	if this.bufferID == "" {
		return fmt.Errorf("Queue not initialised")
	}
	unconv := ctx.Value(this.bufferID)
	if unconv == nil {
		return fmt.Errorf("No kewpie buffer on context")
	}
	buf := unconv.(*buffer)

	*buf = append(*buf, bufferedTask{
		QueueName: queueName,
		Task:      payload,
	})
	return nil
}

func (this Kewpie) Drain(ctx context.Context) error {
	if this.bufferID == "" {
		return fmt.Errorf("Queue not initialised")
	}
	unconv := ctx.Value(this.bufferID)
	if unconv == nil {
		return nil
	}
	buf := unconv.(*buffer)
	errors := []error{}
	for _, bufferedTask := range *buf {
		if err := this.Publish(ctx, bufferedTask.QueueName, bufferedTask.Task); err != nil {
			errors = append(errors, err)
		}
	}
	if len(errors) > 0 {
		var outerErr error
		for _, err := range errors {
			outerErr = fmt.Errorf("%w", err)
		}
		return fmt.Errorf("%w; One or more tasks failed to publish", outerErr)
	}
	return nil
}
