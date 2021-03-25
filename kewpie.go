package kewpie

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/davidbanham/kewpie_go/v3/backends/googlecloudtasks"
	"github.com/davidbanham/kewpie_go/v3/backends/memory"
	"github.com/davidbanham/kewpie_go/v3/backends/postgres"
	"github.com/davidbanham/kewpie_go/v3/backends/sqs"
	"github.com/davidbanham/kewpie_go/v3/types"
	uuid "github.com/satori/go.uuid"
)

type Kewpie struct {
	queues            []string
	backend           Backend
	id                string
	bufferID          string
	publishMiddleware []func(context.Context, *Task, string) error
}

type Task = types.Task
type Tags = types.Tags
type Handler = types.Handler
type HTTPError = types.HTTPError

type bufferedTask struct {
	QueueName string
	Task      *Task
}

type buffer []bufferedTask

type Backend interface {
	Healthy(ctx context.Context) error
	Publish(ctx context.Context, queueName string, payload *types.Task) error
	Subscribe(ctx context.Context, queueName string, handler types.Handler) error
	Suck(ctx context.Context, queueName string, handler types.Handler) error
	Pop(ctx context.Context, queueName string, handler types.Handler) error
	Init(queues []string) error
	Disconnect() error
	Purge(ctx context.Context, queueName string) error
	PurgeMatching(ctx context.Context, queueName, substr string) error
	MaxConcurrentDrainWorkers() int
}

func (this Kewpie) Publish(ctx context.Context, queueName string, payload *types.Task) error {
	if payload.Tags == nil {
		payload.Tags = Tags{}
	}
	// Set RunAt based on the info from Delay
	if payload.Delay != 0 {
		payload.RunAt = time.Now().Add(payload.Delay)
	} else if payload.RunAt.IsZero() {
		payload.RunAt = time.Now()
	}

	if payload.QueueName == "" {
		payload.QueueName = queueName
	}

	payload.Delay = payload.RunAt.Sub(time.Now())

	for _, f := range this.publishMiddleware {
		if err := f(ctx, payload, queueName); err != nil {
			return err
		}
	}

	return this.backend.Publish(ctx, queueName, payload)
}

func (this Kewpie) Subscribe(ctx context.Context, queueName string, handler types.Handler) error {
	return this.backend.Subscribe(ctx, queueName, handler)
}

func (this Kewpie) SubscribeHTTP(secret string, handler types.Handler, errorHandler func(context.Context, types.HTTPError)) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		task := types.Task{}

		if err := task.FromHTTP(r); err != nil {
			errorHandler(r.Context(), types.HTTPError{
				Status: http.StatusBadRequest,
				Error:  fmt.Errorf("Error parsing task: %w", err),
			})
			return
		}

		if task.RunAt.After(time.Now().Add(30 * time.Second)) {
			// We use an internal context here since we don't want the republish to fail if the external is cancelled
			ctx := context.Background()
			if err := this.Publish(ctx, task.QueueName, &task); err != nil {
				errorHandler(r.Context(), types.HTTPError{
					Status: http.StatusInternalServerError,
					Error:  fmt.Errorf("Error republishing future task: %w", err),
				})
				return
			}

			w.WriteHeader(http.StatusCreated)
			w.Write([]byte("rescheduled"))
			return
		}

		if err := task.VerifySignature(secret); err != nil {
			errorHandler(r.Context(), types.HTTPError{
				Status: http.StatusBadRequest,
				Error:  fmt.Errorf("Invalid token: %w", err),
			})
			return
		}

		requeue, handlerErr := handler.Handle(task)
		if handlerErr != nil {
			if !requeue {
				w.WriteHeader(http.StatusAccepted)
				w.Write([]byte("task failed but should not be retried"))
				return
			} else {
				w.WriteHeader(http.StatusInternalServerError)
				w.Write([]byte("error handling task"))
				return
			}
			errorHandler(r.Context(), types.HTTPError{
				Status: http.StatusInternalServerError,
				Error:  handlerErr,
			})
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	}
}

func (this Kewpie) Suck(ctx context.Context, queueName string, handler types.Handler) error {
	return this.backend.Suck(ctx, queueName, handler)
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
	case "google_cloud_tasks":
		this.backend = &googlecloudtasks.CloudTasks{}
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

	errorChan := make(chan error)
	doneChan := make(chan bool)
	jobs := make(chan bufferedTask)

	wg := sync.WaitGroup{}

	workerLimit := this.backend.MaxConcurrentDrainWorkers()

	if len(*buf) < workerLimit {
		workerLimit = len(*buf)
	}

	for id := 1; id <= workerLimit; id++ {
		go drainWorker(ctx, id, this, jobs, doneChan, errorChan)
	}

	go func() {
		for err := range errorChan {
			errors = append(errors, err)
		}
	}()

	go func() {
		for range doneChan {
			wg.Done()
		}
	}()

	for _, bufferedTask := range *buf {
		wg.Add(1)
		jobs <- bufferedTask
	}
	wg.Wait()
	close(jobs)
	close(errorChan)
	close(doneChan)

	if len(errors) > 0 {
		var outerErr error
		for _, err := range errors {
			outerErr = fmt.Errorf("%w", err)
		}
		return fmt.Errorf("%w; One or more tasks failed to publish", outerErr)
	}
	return nil
}

func (this *Kewpie) AddPublishMiddleware(f func(context.Context, *Task, string) error) {
	this.publishMiddleware = append(this.publishMiddleware, f)
}
