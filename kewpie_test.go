package kewpie

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/davidbanham/kewpie_go/types"
	"github.com/davidbanham/required_env"
	"github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

type supDawg struct {
	Sup string
}

var queueName string

var kewpie Kewpie

func init() {
	required_env.Ensure(map[string]string{
		"TEST_QUEUE_NAME": "",
	})

	queueName = os.Getenv("TEST_QUEUE_NAME")

	err := kewpie.Connect("sqs", []string{queueName})
	if err != nil {
		fmt.Printf("DEBUG err: %+v \n", err)
		panic("Error connecting to queue")
	}
}

func TestUnmarshal(t *testing.T) {
	task := types.Task{
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
	handleFunc func(types.Task) (bool, error)
}

func (h *testHandler) Handle(t types.Task) (bool, error) {
	return h.handleFunc(t)
}

func TestSubscribe(t *testing.T) {
	fired := 0
	uniq1 := uuid.NewV4().String()
	uniq2 := uuid.NewV4().String()
	match1 := false
	match2 := false

	handler := &testHandler{
		handleFunc: func(task types.Task) (requeue bool, err error) {
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
	pubTask1 := types.Task{}
	err := pubTask1.Marshal(supDawg{
		Sup: uniq1,
	})
	if err != nil {
		t.Fatal("Err in marshaling")
	}
	pubTask2 := types.Task{}
	err = pubTask2.Marshal(supDawg{
		Sup: uniq2,
	})
	if err != nil {
		t.Fatal("Err in marshaling")
	}
	go kewpie.Subscribe(queueName, handler)
	assert.Nil(t, kewpie.Publish(queueName, pubTask1))
	assert.Nil(t, kewpie.Publish(queueName, pubTask2))
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
