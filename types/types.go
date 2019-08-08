package types

import (
	"database/sql/driver"
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
	Tags         Tags          `json:"tags"`
}

type Tags map[string]string

func (tags Tags) Value() (driver.Value, error) {
	return json.Marshal(tags)
}

func (tags *Tags) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, &tags)
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
var SubscriptionCancelled = errors.New("This subscription has been cancelled")
var NotImplemented = errors.New("This method is not implemented on this backend")
var UnknownBackend = errors.New("I have never heard of that backend")
