package types

import (
	"encoding/json"
	"errors"
	"time"
)

type Task struct {
	ID           string        `json:"id"`
	Body         string        `json:"body"`
	Delay        time.Duration `json:"delay"` // Delay overrides RunAt
	RunAt        time.Time     `json:"run_at"`
	NoExpBackoff bool          `json:"no_exp_backoff"`
	Attempts     int           `json:"attempts"`
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
var ConnectionClosed = errors.New("The connection to the backend is closed")
