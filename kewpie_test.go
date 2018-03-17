package kewpie

import (
	"os"
	"testing"
	"time"

	"github.com/davidbanham/required_env"
	"github.com/satori/go.uuid"
)

type supDawg struct {
	Sup string
}

var queueName string

func init() {
	required_env.Ensure(map[string]string{
		"TEST_QUEUE_NAME": "",
	})

	queueName = os.Getenv("TEST_QUEUE_NAME")

	err := Connect("sqs", []string{queueName})
	if err != nil {
		panic("Error connecting to queue")
	}
}

func TestUnmarshal(t *testing.T) {
	task := Task{
		Body: `{"Sup": "woof"}`,
	}

	woof := supDawg{}

	task.Unmarshal(&woof)

	if woof.Sup != "woof" {
		t.Fail()
	}
}

type testHandler struct {
	fired      bool
	supText    string
	handleFunc func(Task) (bool, error)
}

func (h *testHandler) Handle(t Task) (bool, error) {
	return h.handleFunc(t)
}

func TestSubscribe(t *testing.T) {
	fired := 0
	uniq1 := uuid.NewV4().String()
	uniq2 := uuid.NewV4().String()
	match1 := false
	match2 := false

	handler := &testHandler{
		handleFunc: func(task Task) (requeue bool, err error) {
			fired = fired + 1
			monty := supDawg{}
			task.Unmarshal(&monty)

			if monty.Sup != uniq1 {
				match1 = true
			}
			if monty.Sup != uniq2 {
				match2 = true
			}
			return false, nil
		},
	}
	pubTask1 := Task{}
	err := pubTask1.Marshal(supDawg{
		Sup: uniq1,
	})
	if err != nil {
		t.Fatal("Err in marshaling")
	}
	pubTask2 := Task{}
	err = pubTask2.Marshal(supDawg{
		Sup: uniq2,
	})
	if err != nil {
		t.Fatal("Err in marshaling")
	}
	go Subscribe(queueName, handler)
	Publish(queueName, pubTask1)
	Publish(queueName, pubTask2)
	time.Sleep(1 * time.Second)
	if fired < 2 {
		t.Fatal("Didn't fire enough")
	}
	if !match1 {
		if !match2 {
			t.Fatal("Didn't match either uniq code")
		}
	}
}

func TestCalcDelay(t *testing.T) {
	// It should round long delays to 15 minutes
	originalLongDelay := time.Duration(45 * time.Minute)
	longDelay := roundTo15(originalLongDelay)
	if longDelay != 900*time.Second {
		t.Log("longDelay is", longDelay)
		t.Fatal("longDelay is not 15 minutes")
	}

	// It should not round delays shorter than 15 minutes
	originalShortDelay := time.Duration(6 * time.Minute)
	shortDelay := roundTo15(originalShortDelay)
	if shortDelay != originalShortDelay {
		t.Log("shortDelay is", shortDelay, originalShortDelay)
		t.Fatal("shortDelay was changed")
	}
}
