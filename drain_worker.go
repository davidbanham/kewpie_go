package kewpie

import (
	"context"
)

func drainWorker(ctx context.Context, id int, queue Kewpie, jobs <-chan bufferedTask, done chan<- bool, errors chan<- error) {
	for bt := range jobs {
		if err := queue.Publish(ctx, bt.QueueName, bt.Task); err != nil {
			errors <- err
		}
		done <- true
	}
}
