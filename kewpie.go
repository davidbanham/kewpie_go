package kewpie

import (
	"time"

	"github.com/davidbanham/kewpie_go/backends/memory"
	"github.com/davidbanham/kewpie_go/backends/sqs"
	"github.com/davidbanham/kewpie_go/types"
)

type Kewpie struct {
	queues  []string
	backend Backend
}

type Task = types.Task

type Backend interface {
	Publish(queueName string, payload types.Task) error
	Subscribe(queueName string, handler types.Handler) error
	Init(queues []string) error
}

func (this Kewpie) Publish(queueName string, payload types.Task) (err error) {
	// If Delay is blank, but RunAt is set, populate Delay with info from RunAt
	blankTime := time.Time{}
	if payload.Delay == 0 && payload.RunAt != blankTime {
		payload.Delay = time.Now().Sub(payload.RunAt)
	}

	// Set RunAt based on the info from Delay
	payload.RunAt = time.Now().Add(payload.Delay)

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
	case "memory":
		this.backend = &memory.MemoryStore{}
	case "sqs":
		this.backend = &sqs.Sqs{}
	}

	err = this.backend.Init(queues)
	return
}
