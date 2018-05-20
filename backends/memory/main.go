package memory

import (
	"log"
	"time"

	"github.com/davidbanham/kewpie_go/types"
)

// MemoryStore is intended for use in test suites.
// No data is persisted, so as soon as the process exits all the tasks vanish
// It's written for simplicity over efficiency, so don't expect it to be performant
type MemoryStore struct {
	tasks map[string][]types.Task
}

func (this *MemoryStore) Publish(queueName string, payload types.Task) (err error) {
	if this.tasks[queueName] == nil {
		return types.QueueNotFound
	}

	this.tasks[queueName] = append(this.tasks[queueName], payload)
	return
}

func (this *MemoryStore) Subscribe(queueName string, handler types.Handler) error {
	if this.tasks[queueName] == nil {
		return types.QueueNotFound
	}

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
					this.tasks[queueName] = append(this.tasks[queueName], task)
				}
			}
		}
	}()
	return nil
}

func (this *MemoryStore) Init(queues []string) error {
	this.tasks = make(map[string][]types.Task)
	for _, name := range queues {
		this.tasks[name] = []types.Task{}
	}
	return nil
}
