package types

import (
	"encoding/json"
	"errors"
	"time"
)

type Task struct {
	ID           string
	Body         string
	Delay        time.Duration // Delay overrides RunAt
	RunAt        time.Time
	NoExpBackoff bool
	Attempts     int
}

func (t Task) Unmarshal(res interface{}) (err error) {
	err = json.Unmarshal([]byte(t.Body), res)
	return
}

func (t *Task) Marshal(source interface{}) (err error) {
	byteArr, err := json.Marshal(source)
	t.Body = string(byteArr)
	return
}

type Handler interface {
	Handle(Task) (bool, error)
}

var QueueNotFound = errors.New("I don't know any queue by that name")
