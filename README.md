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

// Here we define a handler function that satisfies the Handler interface kewpie expects.
// It takes in a task and returns an error if the handling fails
// If the error is non-nil, the boolean tells kewpie whether or not the task should be retried
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

	// Marshal serialises our payload struct into the internal JSON representation
	if err := task.Marshal(demoPayload{
		Hello: ", world!",
	}); err != nil {
		log.Fatal(err)
	}

	// In production we'd often use a request context, or a context with a cancel function
	ctx := context.Background()

	if err := queue.Publish(ctx, queueName, &task); err != nil {
		log.Fatal(err)
	}

	// This is just to neatly exit our example code once we've published and consumed a task
	go (func() {
		time.Sleep(1 * time.Second)
		queue.Disconnect()
		os.Exit(0)
	})()

	// Instantiate our handler. In reality we may want to pass a custom handle function or instantiate some other variables on it.
	handler := &demoHandler{}

	// Subscribe to the queue. This will call the handler's Handle method with any tasks we receive
	if err := queue.Subscribe(ctx, queueName, handler); err != nil {
		log.Fatal(err)
	}
}
```
