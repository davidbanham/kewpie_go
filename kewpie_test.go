package kewpie

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/davidbanham/kewpie_go/v3/types"
	"github.com/davidbanham/required_env"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

//var backends = []string{"sqs"}
//var backends = []string{"memory", "postgres", "sqs"}
var backends = []testBackend{testBackend{"memory", nil}, testBackend{"postgres", nil}}

//var backends = []testBackend{testBackend{"google_cloud_tasks", nil}}

type testBackend struct {
	Identifier string
	Connection *sql.DB
}

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

		if backend.Identifier == "postgres" {
			db, err := sql.Open("postgres", os.Getenv("DB_URI"))
			if err != nil {
				log.Fatal(err)
			}
			backend.Connection = db
		}

		if err := kewpie.Connect(backend.Identifier, []string{queueName}, backend.Connection); err != nil {
			log.Fatal(err)
		}

		if err := kewpie.Purge(context.Background(), queueName); err != nil {
			if err != types.NotImplemented {
				log.Fatal(err)
			}
		}

		log.Println("Purged")
	}
}

func TestPGInit(t *testing.T) {
	kewpie := Kewpie{}
	assert.Nil(t, kewpie.Connect("postgres", []string{queueName}, nil))
}

func TestUnmarshal(t *testing.T) {
	task := types.Task{
		Body: `{"Sup": "woof"}`,
		Tags: map[string]string{"handler_url": "http://example.com"},
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

func TestSubscribeSimple(t *testing.T) {
	for _, backend := range backends {
		kewpie := Kewpie{}

		if err := kewpie.Connect(backend.Identifier, []string{queueName}, backend.Connection); err != nil {
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
			Tags:         map[string]string{"handler_url": "http://example.com"},
			NoExpBackoff: true,
		}
		err := pubTask1.Marshal(supDawg{
			Sup: uniq1,
		})
		if err != nil {
			t.Fatal("Err in marshaling")
		}
		pubTask2 := types.Task{
			Tags:         map[string]string{"handler_url": "http://example.com"},
			NoExpBackoff: true,
		}
		err = pubTask2.Marshal(supDawg{
			Sup: uniq2,
		})
		if err != nil {
			t.Fatal("Err in marshaling")
		}

		ctx, cancel := context.WithCancel(context.Background())
		go (func() {
			log.Println("About to subscribe")
			assert.Contains(t, []error{types.SubscriptionCancelled, types.NotImplemented}, kewpie.Subscribe(ctx, queueName, handler))
		})()
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

		cancel()
	}
}

func TestBuffer(t *testing.T) {
	for _, backend := range backends {
		kewpie := Kewpie{}

		if err := kewpie.Connect(backend.Identifier, []string{queueName}, backend.Connection); err != nil {
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
			Tags:         map[string]string{"handler_url": "http://example.com"},
			NoExpBackoff: true,
		}
		err := pubTask1.Marshal(supDawg{
			Sup: uniq1,
		})
		if err != nil {
			t.Fatal("Err in marshaling")
		}
		pubTask2 := types.Task{
			Tags:         map[string]string{"handler_url": "http://example.com"},
			NoExpBackoff: true,
		}
		err = pubTask2.Marshal(supDawg{
			Sup: uniq2,
		})
		if err != nil {
			t.Fatal("Err in marshaling")
		}

		ctx, cancel := context.WithCancel(context.Background())
		go (func() {
			assert.Contains(t, []error{types.SubscriptionCancelled, types.NotImplemented}, kewpie.Subscribe(ctx, queueName, handler))
		})()
		ctx = kewpie.PrepareContext(ctx)
		assert.Nil(t, kewpie.Buffer(ctx, queueName, &pubTask1))
		assert.Nil(t, kewpie.Buffer(ctx, queueName, &pubTask2))
		time.Sleep(2 * time.Second)
		if fired > 0 {
			t.Fatal("Should not have fired yet")
		}
		assert.Nil(t, kewpie.Drain(ctx))
		time.Sleep(5 * time.Second)
		if fired < 2 {
			t.Fatal("Didn't fire enough", backend)
		}
		if !match1 {
			if !match2 {
				t.Fatal("Didn't match either uniq code", backend)
			}
		}

		cancel()
	}
}

func TestSubscribeFailures(t *testing.T) {
	// Ensure that when a pod fails a task it continues to consume future jobs
	for _, backend := range backends {
		kewpie := Kewpie{}

		if err := kewpie.Connect(backend.Identifier, []string{queueName}, backend.Connection); err != nil {
			log.Fatal("Error connecting to queue")
		}

		fired := 0
		uniq1 := uuid.NewV4().String()

		handler := &testHandler{
			handleFunc: func(task types.Task) (requeue bool, err error) {
				fired += 1
				return false, fmt.Errorf("Something bad happen")
			},
		}
		pubTask1 := types.Task{
			Tags:         map[string]string{"handler_url": "http://example.com"},
			NoExpBackoff: true,
		}
		err := pubTask1.Marshal(supDawg{
			Sup: uniq1,
		})
		if err != nil {
			t.Fatal("Err in marshaling")
		}

		ctx, cancel := context.WithCancel(context.Background())
		go (func() {
			assert.Contains(t, []error{types.SubscriptionCancelled, types.NotImplemented}, kewpie.Subscribe(ctx, queueName, handler))
		})()
		assert.Nil(t, kewpie.Publish(ctx, queueName, &pubTask1))
		time.Sleep(2 * time.Second)
		assert.Nil(t, kewpie.Publish(ctx, queueName, &pubTask1))
		assert.Nil(t, kewpie.Publish(ctx, queueName, &pubTask1))
		assert.Nil(t, kewpie.Publish(ctx, queueName, &pubTask1))

		time.Sleep(8 * time.Second)

		if fired < 4 {
			t.Fatal("Didn't fire enough", backend)
		}

		cancel()
	}
}

func TestPop(t *testing.T) {
	for _, backend := range backends {
		kewpie := Kewpie{}

		if err := kewpie.Connect(backend.Identifier, []string{queueName}, backend.Connection); err != nil {
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
			Tags:         map[string]string{"handler_url": "http://example.com"},
			NoExpBackoff: true,
		}
		err := pubTask1.Marshal(supDawg{
			Sup: uniq1,
		})
		if err != nil {
			t.Fatal("Err in marshaling")
		}
		pubTask2 := types.Task{
			Tags:         map[string]string{"handler_url": "http://example.com"},
			NoExpBackoff: true,
		}
		err = pubTask2.Marshal(supDawg{
			Sup: uniq2,
		})
		if err != nil {
			t.Fatal("Err in marshaling")
		}

		ctx, cancel := context.WithCancel(context.Background())
		go (func() {
			assert.Nil(t, kewpie.Pop(ctx, queueName, handler))
		})()
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

		cancel()
	}
}

func TestRequeueing(t *testing.T) {
	for _, backend := range backends {

		kewpie := Kewpie{}

		if err := kewpie.Connect(backend.Identifier, []string{queueName}, backend.Connection); err != nil {
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
					return true, fmt.Errorf("Keep going!" + backend.Identifier)
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
		pubTask := types.Task{
			Tags: map[string]string{"handler_url": "http://example.com"},
		}
		err := pubTask.Marshal(supDawg{
			Sup: uniq,
		})
		if err != nil {
			t.Fatal("Err in marshaling")
		}
		pubTask2 := types.Task{
			Tags: map[string]string{"handler_url": "http://example.com"},
		}
		err = pubTask2.Marshal(supDawg{
			Sup: uniq2,
		})
		if err != nil {
			t.Fatal("Err in marshaling")
		}
		pubTask3 := types.Task{
			Tags: map[string]string{"handler_url": "http://example.com"},
		}
		err = pubTask3.Marshal(supDawg{
			Sup: uniq3,
		})
		if err != nil {
			t.Fatal("Err in marshaling")
		}

		pubTask.NoExpBackoff = true
		pubTask2.NoExpBackoff = true
		pubTask3.NoExpBackoff = true

		ctx, cancel := context.WithCancel(context.Background())
		go (func() {
			assert.Contains(t, []error{types.SubscriptionCancelled, types.NotImplemented}, kewpie.Subscribe(ctx, queueName, handler))
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

		cancel()
	}
}

func TestPurgeMatching(t *testing.T) {
	for _, backend := range backends {
		kewpie := Kewpie{}

		if err := kewpie.Connect(backend.Identifier, []string{queueName}, backend.Connection); err != nil {
			log.Fatal("Error connecting to queue")
		}

		uniq1 := uuid.NewV4().String()
		uniq2 := uuid.NewV4().String()
		match1 := false
		match2 := false

		pubTask1 := types.Task{
			NoExpBackoff: true,
			Tags:         map[string]string{"handler_url": "http://example.com"},
		}
		assert.Nil(t, pubTask1.Marshal(supDawg{
			Sup: uniq1,
		}))
		pubTask2 := types.Task{
			NoExpBackoff: true,
			Tags:         map[string]string{"handler_url": "http://example.com"},
		}
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

		ctx, cancel := context.WithCancel(context.Background())
		assert.Nil(t, kewpie.Publish(ctx, queueName, &pubTask1))
		assert.Nil(t, kewpie.Publish(ctx, queueName, &pubTask2))

		if purgeErr := kewpie.PurgeMatching(ctx, queueName, uniq1); purgeErr == nil {
			go (func() {
				assert.Contains(t, []error{types.SubscriptionCancelled, types.NotImplemented}, kewpie.Subscribe(ctx, queueName, handler))
			})()

			time.Sleep(5 * time.Second)

			assert.False(t, match1)
			assert.True(t, match2)
		} else {
			assert.Equal(t, purgeErr, types.NotImplemented)
		}

		cancel()
	}
}

func TestTags(t *testing.T) {
	for _, backend := range backends {
		kewpie := Kewpie{}

		if err := kewpie.Connect(backend.Identifier, []string{queueName}, backend.Connection); err != nil {
			log.Fatal("Error connecting to queue")
		}

		hit := false

		pubTask1 := types.Task{
			Tags: types.Tags{
				"name":        "monty",
				"handler_url": "http://example.com",
			},
			NoExpBackoff: true,
		}
		assert.Nil(t, pubTask1.Marshal(supDawg{
			Sup: "tags",
		}))

		handler := &testHandler{
			handleFunc: func(task types.Task) (requeue bool, err error) {
				monty := supDawg{}
				task.Unmarshal(&monty)

				if monty.Sup == "tags" {
					assert.Equal(t, task.Tags["name"], "monty")
					hit = true
				}

				return false, nil
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		assert.Nil(t, kewpie.Publish(ctx, queueName, &pubTask1))

		go (func() {
			assert.Contains(t, []error{types.SubscriptionCancelled, types.NotImplemented}, kewpie.Subscribe(ctx, queueName, handler))
		})()
		time.Sleep(1 * time.Second)
		assert.True(t, hit)

		cancel()
	}
}

func TestCancel(t *testing.T) {
	for _, backend := range backends {
		hangTime := 1100 * time.Millisecond
		if backend.Identifier == "sqs" {
			hangTime = 21 * time.Second
		}
		kewpie := Kewpie{}

		if err := kewpie.Connect(backend.Identifier, []string{queueName}, backend.Connection); err != nil {
			log.Fatal("Error connecting to queue")
		}

		ctx, cancel := context.WithCancel(context.Background())

		handler := &testHandler{
			handleFunc: func(task types.Task) (requeue bool, err error) {
				return false, nil
			},
		}

		hit := false
		go (func() {
			assert.Contains(t, []error{types.SubscriptionCancelled, types.NotImplemented}, kewpie.Subscribe(ctx, queueName, handler))
			hit = true
		})()

		cancel()
		time.Sleep(hangTime)
		assert.True(t, hit)

		assert.Nil(t, kewpie.Disconnect())
	}
}

func TestPublishMiddleware(t *testing.T) {
	kewpie := Kewpie{}

	if err := kewpie.Connect("memory", []string{queueName}, nil); err != nil {
		log.Fatal("Error connecting to queue")
	}

	uniq1 := uuid.NewV4().String()
	uniq2 := uuid.NewV4().String()

	middlewareFired := false

	kewpie.AddPublishMiddleware(func(ctx context.Context, task *Task, passedQueueName string) error {
		assert.Equal(t, passedQueueName, queueName)
		task.Tags["middleware"] = uniq2
		middlewareFired = true
		return nil
	})

	pubTask1 := types.Task{
		Tags:         map[string]string{"handler_url": "http://example.com"},
		NoExpBackoff: true,
	}
	if err := pubTask1.Marshal(supDawg{
		Sup: uniq1,
	}); err != nil {
		t.Fatal("Err in marshaling")
	}

	ctx, cancel := context.WithCancel(context.Background())

	assert.Nil(t, kewpie.Publish(ctx, queueName, &pubTask1))

	fired := 0
	var match1, match2 bool

	handler := &testHandler{
		handleFunc: func(task types.Task) (requeue bool, err error) {
			fired = fired + 1
			monty := supDawg{}
			task.Unmarshal(&monty)

			if monty.Sup == uniq1 {
				match1 = true
			}
			if task.Tags["middleware"] == uniq2 {
				match2 = true
			}
			return false, nil
		},
	}

	go (func() {
		assert.Equal(t, types.NotImplemented, kewpie.Subscribe(ctx, queueName, handler))
	})()

	time.Sleep(1 * time.Second)
	if fired < 1 {
		t.Log("Didn't fire enough")
		t.Fail()
	}
	if !match1 {
		t.Log("Didn't match 1")
		t.Fail()
	}
	if !match2 {
		t.Log("Didn't match 2")
		t.Fail()
	}
	if !middlewareFired {
		t.Log("Middleware didn't fire")
		t.Fail()
	}

	cancel()
}

func TestSubscribeHTTP(t *testing.T) {
	kewpie := Kewpie{}

	if err := kewpie.Connect("memory", []string{queueName}, nil); err != nil {
		log.Fatal("Error connecting to queue")
	}

	middlewareFired := false

	inOneHour := time.Now().Add(time.Hour)

	kewpie.AddPublishMiddleware(func(ctx context.Context, task *Task, passedQueueName string) error {
		assert.True(t, inOneHour.Equal(task.RunAt))
		assert.InDelta(t, task.Delay, time.Hour, float64(30*time.Second))
		middlewareFired = true
		return nil
	})

	secret := uuid.NewV4().String()

	taskHandler := &testHandler{
		handleFunc: func(task types.Task) (requeue bool, err error) {
			assert.Fail(t, "Handler should not fire")
			return false, nil
		},
	}

	errorHandler := func(ctx context.Context, err types.HTTPError) {
		assert.FailNow(t, "Erorr handler called", err)
	}

	handler := kewpie.SubscribeHTTP(secret, taskHandler, errorHandler)

	task := types.Task{
		QueueName: "foo",
		Body:      `{"Sup": "woof"}`,
		Tags:      map[string]string{"handler_url": "http://example.com"},
		RunAt:     inOneHour,
		Delay:     time.Hour * 24 * 30,
	}

	task.Sign(secret)

	payload, err := json.Marshal(task)
	assert.Nil(t, err)

	rr := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "http://example.com/foo", bytes.NewReader(payload))

	go (func() {
		handler(rr, req)
	})()
	time.Sleep(1 * time.Second)
	assert.True(t, middlewareFired)
}
