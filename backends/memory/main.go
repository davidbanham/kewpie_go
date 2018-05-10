package memory

import (
	"log"

	"github.com/davidbanham/kewpie_go/types"
)

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
				continue
			}
			task := this.tasks[queueName][0]
			this.tasks[queueName] = append(this.tasks[queueName][:0], this.tasks[queueName][1:]...)

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
