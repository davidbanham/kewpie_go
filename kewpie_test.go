package kewpie

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/davidbanham/kewpie_go/types"
	"github.com/davidbanham/required_env"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

//var backends = []string{"memory", "sqs", "postgres"}
var backends = []string{"memory", "postgres"}

type supDawg struct {
	Sup string
}

var queueName string

func init() {
	required_env.Ensure(map[string]string{
		"TEST_QUEUE_NAME": "",
	})

	queueName = os.Getenv("TEST_QUEUE_NAME")

	for _, backend := range backends {
		kewpie := Kewpie{}

		if err := kewpie.Connect(backend, []string{queueName}); err != nil {
			log.Fatal(err)
		}

		if err := kewpie.Purge(context.Background(), queueName); err != nil {
			log.Fatal(err)
		}
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
	for _, backend := range backends {
		kewpie := Kewpie{}

		if err := kewpie.Connect(backend, []string{queueName}); err != nil {
			log.Fatal("Error connecting to queue")
		}

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

				if monty.Sup == uniq1 {
					match1 = true
				}
				if monty.Sup == uniq2 {
					match2 = true
				}
				return false, nil
			},
		}
		pubTask1 := types.Task{
			NoExpBackoff: true,
		}
		err := pubTask1.Marshal(supDawg{
			Sup: uniq1,
		})
		if err != nil {
			t.Fatal("Err in marshaling")
		}
		pubTask2 := types.Task{
			NoExpBackoff: true,
		}
		err = pubTask2.Marshal(supDawg{
			Sup: uniq2,
		})
		if err != nil {
			t.Fatal("Err in marshaling")
		}

		ctx := context.Background()
		go kewpie.Subscribe(ctx, queueName, handler)
		assert.Nil(t, kewpie.Publish(ctx, queueName, &pubTask1))
		assert.Nil(t, kewpie.Publish(ctx, queueName, &pubTask2))
		time.Sleep(5 * time.Second)
		if fired < 2 {
			t.Fatal("Didn't fire enough", backend)
		}
		if !match1 {
			if !match2 {
				t.Fatal("Didn't match either uniq code", backend)
			}
		}

		assert.Nil(t, kewpie.Disconnect())
	}
}

func TestPop(t *testing.T) {
	for _, backend := range backends {
		kewpie := Kewpie{}

		if err := kewpie.Connect(backend, []string{queueName}); err != nil {
			log.Fatal("Error connecting to queue")
		}

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

				if monty.Sup == uniq1 {
					match1 = true
				}
				if monty.Sup == uniq2 {
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

		ctx := context.Background()
		go kewpie.Pop(ctx, queueName, handler)
		assert.Nil(t, kewpie.Publish(ctx, queueName, &pubTask1))
		assert.Nil(t, kewpie.Publish(ctx, queueName, &pubTask2))
		time.Sleep(5 * time.Second)
		if fired < 1 {
			t.Fatal("Didn't fire enough", backend)
		}
		if fired > 1 {
			t.Fatal("Fired too much", backend)
		}
		if !match1 {
			if !match2 {
				t.Fatal("Didn't match either uniq code", backend)
			}
		}

		assert.Nil(t, kewpie.Disconnect())
	}
}

func TestRequeueing(t *testing.T) {
	for _, backend := range backends {

		kewpie := Kewpie{}

		if err := kewpie.Connect(backend, []string{queueName}); err != nil {
			log.Fatal("Error connecting to queue")
		}

		uniq := "cycler" + uuid.NewV4().String()
		matched := 0
		uniq2 := uuid.NewV4().String()
		matched2 := 0
		uniq3 := uuid.NewV4().String()
		matched3 := 0

		handler := &testHandler{
			handleFunc: func(task types.Task) (requeue bool, err error) {
				monty := supDawg{}
				task.Unmarshal(&monty)

				if monty.Sup == uniq {
					matched += 1
					if task.Attempts > 3 {
						return false, nil
					}
					return true, fmt.Errorf("Keep going!" + backend)
				}
				if monty.Sup == uniq2 {
					matched2 += 1
					return false, fmt.Errorf("That'll do")
				}
				if monty.Sup == uniq3 {
					matched3 += 1
					return false, nil
				}

				return false, nil
			},
		}
		pubTask := types.Task{}
		err := pubTask.Marshal(supDawg{
			Sup: uniq,
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
		pubTask3 := types.Task{}
		err = pubTask3.Marshal(supDawg{
			Sup: uniq3,
		})
		if err != nil {
			t.Fatal("Err in marshaling")
		}

		pubTask.NoExpBackoff = true
		pubTask2.NoExpBackoff = true
		pubTask3.NoExpBackoff = true

		ctx := context.Background()
		go (func() {
			kewpie.Subscribe(ctx, queueName, handler)
		})()
		assert.Nil(t, kewpie.Publish(ctx, queueName, &pubTask))
		assert.Nil(t, kewpie.Publish(ctx, queueName, &pubTask2))
		assert.Nil(t, kewpie.Publish(ctx, queueName, &pubTask3))
		time.Sleep(5 * time.Second)
		if matched < 2 {
			t.Error("Didn't fire enough", matched, backend)
		}
		assert.Equal(t, 1, matched2, backend)
		assert.Equal(t, 1, matched3, backend)

		assert.Nil(t, kewpie.Disconnect())
	}
}

func TestPurgeMatching(t *testing.T) {
	for _, backend := range backends {
		kewpie := Kewpie{}

		if err := kewpie.Connect(backend, []string{queueName}); err != nil {
			log.Fatal("Error connecting to queue")
		}

		uniq1 := uuid.NewV4().String()
		uniq2 := uuid.NewV4().String()
		match1 := false
		match2 := false

		pubTask1 := types.Task{}
		assert.Nil(t, pubTask1.Marshal(supDawg{
			Sup: uniq1,
		}))
		pubTask2 := types.Task{}
		assert.Nil(t, pubTask2.Marshal(supDawg{
			Sup: uniq2,
		}))

		handler := &testHandler{
			handleFunc: func(task types.Task) (requeue bool, err error) {
				monty := supDawg{}
				task.Unmarshal(&monty)

				if monty.Sup == uniq1 {
					match1 = true
				}
				if monty.Sup == uniq2 {
					match2 = true
				}
				return false, nil
			},
		}

		ctx := context.Background()
		assert.Nil(t, kewpie.Publish(ctx, queueName, &pubTask1))
		assert.Nil(t, kewpie.Publish(ctx, queueName, &pubTask2))

		if purgeErr := kewpie.PurgeMatching(ctx, queueName, uniq1); purgeErr == nil {
			go kewpie.Subscribe(ctx, queueName, handler)

			time.Sleep(5 * time.Second)

			assert.False(t, match1)
			assert.True(t, match2)
		} else {
			assert.Equal(t, purgeErr, types.NotImplemented)
		}

		assert.Nil(t, kewpie.Disconnect())
	}
}
