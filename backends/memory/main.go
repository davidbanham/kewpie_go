package memory

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/davidbanham/kewpie_go/types"
	uuid "github.com/satori/go.uuid"
)

// MemoryStore is intended for use in test suites.
// No data is persisted, so as soon as the process exits all the tasks vanish
// It's written for simplicity over efficiency, so don't expect it to be performant
type MemoryStore struct {
	tasks  map[string][]types.Task
	closed bool
}

func (this *MemoryStore) Publish(ctx context.Context, queueName string, payload *types.Task) (err error) {
	if this.tasks[queueName] == nil {
		return types.QueueNotFound
	}

	payload.ID = uuid.NewV4().String()
	this.tasks[queueName] = append(this.tasks[queueName], *payload)
	return
}

func (this *MemoryStore) Pop(ctx context.Context, queueName string, handler types.Handler) error {
	if this.closed {
		return fmt.Errorf("Connection closed")
	}

	if this.tasks[queueName] == nil {
		return types.QueueNotFound
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for {
			if len(this.tasks[queueName]) == 0 {
				time.Sleep(1 * time.Second)
				continue
			}
			task := this.tasks[queueName][0]
			this.tasks[queueName] = append(this.tasks[queueName][:0], this.tasks[queueName][1:]...)

			// Chuck it back on the end of the queue if it's not due to run yet
			if time.Now().Before(task.RunAt) {
				this.tasks[queueName] = append(this.tasks[queueName], task)
				time.Sleep(1 * time.Second)
				continue
			}

			requeue, err := handler.Handle(task)
			if err != nil {
				log.Println("ERROR kewpie task handler", err)
				if requeue {
					task.Attempts += 1
					this.tasks[queueName] = append(this.tasks[queueName], task)
				}
			}
			wg.Done()
			return
		}
	}()
	wg.Wait()
	return nil
}

func (this *MemoryStore) Subscribe(ctx context.Context, queueName string, handler types.Handler) error {
	for {
		if this.closed {
			return nil
		}

		if err := this.Pop(ctx, queueName, handler); err != nil {
			return err
		}
	}
}

func (this *MemoryStore) Init(queues []string) error {
	this.tasks = make(map[string][]types.Task)
	for _, name := range queues {
		this.tasks[name] = []types.Task{}
	}
	return nil
}

func (this *MemoryStore) Disconnect() error {
	this.closed = true
	return nil
}

func (this *MemoryStore) Purge(ctx context.Context, queueName string) error {
	if this.closed {
		return fmt.Errorf("Connection closed")
	}

	this.tasks[queueName] = []types.Task{}

	return nil
}
