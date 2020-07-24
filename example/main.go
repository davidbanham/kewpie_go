package main

import (
	"context"
	"log"
	"os"
	"time"

	kewpie "github.com/davidbanham/kewpie_go/v3"
)

type demoPayload struct {
	Hello string
}

type demoHandler struct{}

func (demoHandler) Handle(task kewpie.Task) (bool, error) {
	payload := demoPayload{}

	if err := task.Unmarshal(&payload); err != nil {
		// If the task is malformed, don't attempt to retry it
		return false, err
	}

	log.Println("Hello", payload.Hello)

	return false, nil
}

func main() {
	queueName := "demo"

	queue := kewpie.Kewpie{}

	if err := queue.Connect("postgres", []string{queueName}, nil); err != nil {
		log.Fatal(err)
	}

	task := kewpie.Task{}

	if err := task.Marshal(demoPayload{
		Hello: ", world!",
	}); err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	if err := queue.Publish(ctx, queueName, &task); err != nil {
		log.Fatal(err)
	}

	go (func() {
		time.Sleep(1 * time.Second)
		queue.Disconnect()
		os.Exit(0)
	})()

	handler := &demoHandler{}

	if err := queue.Subscribe(ctx, queueName, handler); err != nil {
		log.Fatal(err)
	}
}
