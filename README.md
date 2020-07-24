## Kewpie

Kewpie is a task queue abstraction. It supports pluggable backends with a single common interface.

The format of tasks stored in backends is designed to be simple to write and consume, making it easy to write kewpie libraries in many languages.

The currently supported backends are:
1. Amazon SQS
2. PostgreSQL
3. Memory (designed for testing only)
4. Google PubSub (in progress, not yet ready for production)

### Example

``` Go
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
```
