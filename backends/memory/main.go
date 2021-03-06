package memory

import (
	"context"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/davidbanham/kewpie_go/v3/types"
	"github.com/davidbanham/kewpie_go/v3/util"
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
	cancelled := false

	go func() {
		for {
			select {
			case <-ctx.Done():
				cancelled = true
				return
			}
		}
	}()

	if this.closed {
		return types.ConnectionClosed
	}

	if this.tasks[queueName] == nil {
		return types.QueueNotFound
	}

	var wg sync.WaitGroup
	wg.Add(1)
	var innerErr error
	go func() {
		for {
			if cancelled {
				innerErr = types.SubscriptionCancelled
				wg.Done()
				return
			}

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
					if !task.NoExpBackoff {
						task.Delay = util.CalcBackoff(task.Attempts + 1)
						task.RunAt = time.Now().Add(task.Delay)
					}
					this.tasks[queueName] = append(this.tasks[queueName], task)
				}
			}
			wg.Done()
			return
		}
	}()
	wg.Wait()
	return innerErr
}

func (this *MemoryStore) Suck(ctx context.Context, queueName string, handler types.Handler) error {
	return types.NotImplemented
}

func (this *MemoryStore) Subscribe(ctx context.Context, queueName string, handler types.Handler) error {
	for {
		if this.closed {
			return types.ConnectionClosed
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
		return types.ConnectionClosed
	}

	this.tasks[queueName] = []types.Task{}

	return nil
}

func (this *MemoryStore) PurgeMatching(ctx context.Context, queueName, substr string) error {
	for i, task := range this.tasks[queueName] {
		if strings.Contains(task.Body, substr) {
			this.tasks[queueName] = append(this.tasks[queueName][:i], this.tasks[queueName][i+1:]...)
		}
	}
	return nil
}

func (this MemoryStore) Healthy(ctx context.Context) error {
	return nil
}

func (this MemoryStore) MaxConcurrentDrainWorkers() int {
	return 10
}
