package googlepubsub

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/davidbanham/kewpie_go/v3/types"
	uuid "github.com/satori/go.uuid"
)

type PubSub struct {
	topics map[string]*pubsub.Topic
	client *pubsub.Client
	closed bool
}

func (this PubSub) Publish(ctx context.Context, queueName string, payload *types.Task) error {
	if _, ok := this.topics[queueName]; !ok {
		return types.QueueNotFound
	}

	topic := this.topics[queueName]

	attrs := payload.Tags
	if attrs == nil {
		attrs = map[string]string{}
	}
	attrs["kewpie_run_at"] = payload.RunAt.Format(time.RFC3339)

	res := topic.Publish(ctx, &pubsub.Message{
		Data:       []byte(payload.Body),
		Attributes: attrs,
	})

	id, err := res.Get(ctx)
	if err != nil {
		return err
	}

	payload.ID = id

	return nil
}

func (this PubSub) Pop(ctx context.Context, queueName string, handler types.Handler) error {
	return types.NotImplemented

	if this.closed {
		return types.ConnectionClosed
	}

	sub, err := this.client.CreateSubscription(context.Background(), uuid.NewV4().String(), pubsub.SubscriptionConfig{Topic: this.topics[queueName]})

	if err != nil {
		return err
	}

	//sub.ReceiveSettings.MaxOutstandingMessages = 1

	var handlerErr error

	subCtx, cancelFunc := context.WithCancel(ctx)

	err = sub.Receive(subCtx, func(innerCtx context.Context, msg *pubsub.Message) {
		task := types.Task{
			Body: string(msg.Data),
			ID:   msg.ID,
			Tags: msg.Attributes,
		}

		if msg.Attributes["kewpie_run_at"] != "" {
			parsed, err := time.Parse(msg.Attributes["kewpie_run_at"], time.RFC3339)
			if err != nil {
				msg.Nack()
				handlerErr = err
				return
			}
			now := time.Now()
			if now.Before(parsed.Add(-time.Second)) {
				msg.Nack()
				return
			}
			task.RunAt = parsed
		}

		requeue, err := handler.Handle(task)

		if err == nil {
			msg.Ack()
			cancelFunc()
			return
		}

		if !requeue {
			msg.Ack()
		} else {
			msg.Nack()
		}

		cancelFunc()
		handlerErr = err
		return
	})

	if handlerErr != nil {
		return handlerErr
	}

	return err
}

func (this PubSub) Subscribe(ctx context.Context, queueName string, handler types.Handler) (err error) {
	for {
		if this.closed {
			return types.ConnectionClosed
		}
		if err := this.Pop(ctx, queueName, handler); err != nil {
			return err
		}
	}
	return nil
}

func (this *PubSub) Init(queues []string) (err error) {
	this.topics = map[string]*pubsub.Topic{}

	projectID := os.Getenv("GOOGLE_CLOUD_PROJECT_ID")
	if projectID == "" {
		return errors.New("Must set env var GOOGLE_CLOUD_PROJECT_ID")
	}

	ctx := context.Background()
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		return err
	}

	this.client = client

	for _, queue := range queues {
		topic, err := this.client.CreateTopic(ctx, queue)
		if err != nil {
			if strings.Contains(err.Error(), "AlreadyExists") {
				topic = client.Topic(queue)
			} else {
				return err
			}
		}
		this.topics[queue] = topic
	}

	return nil
}

func (this *PubSub) Disconnect() error {
	this.closed = true
	for _, topic := range this.topics {
		topic.Stop()
	}
	return nil
}

func (this PubSub) Purge(ctx context.Context, queueName string) error {
	err := this.topics[queueName].Delete(ctx)
	if err != nil {
		return err
	}
	topic, err := this.client.CreateTopic(ctx, queueName)
	if err != nil {
		return err
	}
	this.topics[queueName] = topic
	return nil
}

func (this PubSub) PurgeMatching(ctx context.Context, queueName, substr string) error {
	return types.NotImplemented
}

func (this PubSub) Healthy(ctx context.Context) error {
	for _, topic := range this.topics {
		_, err := topic.Update(ctx, pubsub.TopicConfigToUpdate{})
		if err != nil {
			return err
		}
	}
	return nil
}
