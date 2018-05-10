package kewpie

import (
	"github.com/davidbanham/kewpie_go/backends/sqs"
	"github.com/davidbanham/kewpie_go/types"
)

type Kewpie struct {
	queues  []string
	backend Backend
}

type Backend interface {
	Publish(queueName string, payload types.Task) error
	Subscribe(queueName string, handler types.Handler) error
	Init(queues []string) error
}

func (this Kewpie) Publish(queueName string, payload types.Task) (err error) {
	err = this.backend.Publish(queueName, payload)

	return
}

func (this Kewpie) Subscribe(queueName string, handler types.Handler) (err error) {
	err = this.backend.Subscribe(queueName, handler)
	return
}

func (this *Kewpie) Connect(backend string, queues []string) (err error) {
	this.queues = queues

	switch backend {
	case "test":
		return
	case "sqs":
		this.backend = &sqs.Sqs{}
	}

	err = this.backend.Init(queues)
	return
}
