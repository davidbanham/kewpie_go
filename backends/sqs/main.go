package sqs

import (
	"context"
	"log"
	"path"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/davidbanham/kewpie_go/types"
	"github.com/davidbanham/kewpie_go/util"
)

type Sqs struct {
	urls   map[string]string
	svc    *sqs.SQS
	closed bool
}

const FIFTEEN_MINUTES = (15 * time.Minute)

func roundTo15(orig time.Duration) time.Duration {
	if orig > FIFTEEN_MINUTES {
		return time.Duration(FIFTEEN_MINUTES)
	}
	return orig
}

func (this Sqs) Publish(ctx context.Context, queueName string, payload types.Task) (err error) {
	url := this.urls[queueName]
	if url == "" {
		err = types.QueueNotFound
		return
	}

	log.Println("DEBUG kewpie", queueName, "Publishing task", payload)

	noExpBackoff := "false"
	if payload.NoExpBackoff {
		noExpBackoff = "true"
	}

	delay := int64(roundTo15(payload.Delay).Seconds())

	runAt := time.Now().Add(payload.Delay)

	message := sqs.SendMessageInput{
		DelaySeconds: &delay,
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"RunAt": &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(runAt.UTC().Format(time.RFC3339)),
			},
			"NoExpBackoff": &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(noExpBackoff),
			},
			"Attempts": &sqs.MessageAttributeValue{
				DataType:    aws.String("Number"),
				StringValue: aws.String("0"),
			},
		},
		MessageBody: &payload.Body,
		QueueUrl:    &url,
	}
	_, err = this.svc.SendMessage(&message)
	return
}

func (this Sqs) Subscribe(ctx context.Context, queueName string, handler types.Handler) (err error) {
	if this.closed {
		return nil
	}

	url := this.urls[queueName]
	if url == "" {
		err = types.QueueNotFound
		return
	}

	twentySeconds := int64(20)
	ninetySeconds := int64(90)

	params := sqs.ReceiveMessageInput{
		QueueUrl:          &url,
		VisibilityTimeout: &ninetySeconds,
		WaitTimeSeconds:   &twentySeconds,
		MessageAttributeNames: []*string{
			aws.String("RunAt"),
			aws.String("NoExpBackoff"),
		},
	}
	response, err := this.svc.ReceiveMessage(&params)
	if err != nil {
		log.Println("ERROR kewpie", queueName, "Error recieving message from queue", queueName, err)
		return
	}
	for _, message := range response.Messages {
		task := types.Task{
			Body: *message.Body,
		}
		log.Println("INFO kewpie Received task from queue", queueName)
		log.Println("DEBUG kewpie Received task from queue", queueName, task)

		runAtPtr := message.MessageAttributes["RunAt"]
		attemptsPtr := message.MessageAttributes["Attempts"]
		attempts := 0
		noExpBackoffPtr := message.MessageAttributes["NoExpBackoff"]
		noExpBackoff := false

		if runAtPtr == nil {
			log.Println("ERROR kewpie", queueName, "RunAt was nil", message)
			continue
		}

		if noExpBackoffPtr != nil && noExpBackoffPtr.String() == "true" {
			noExpBackoff = true
		}

		if attemptsPtr != nil {
			parsed, err := strconv.Atoi(attemptsPtr.String())
			if err != nil {
				log.Println("ERROR kewpie", queueName, "Attempts was not an int", message, err)
				continue
			}
			attempts = parsed
		}

		runAtString := *runAtPtr.StringValue

		runAt, err := time.Parse(time.RFC3339, runAtString)
		if err != nil {
			log.Println("ERROR kewpie", queueName, "Error decoding runAt from message", message)
			continue
		}
		now := time.Now()
		// Check if the task should run now or in the future
		// Some queues have a max delay. This allows us to just republish a message with a longer delay than the max.
		// Knock runAt back 1s to avoid off-by-one error on comparison
		if now.Before(runAt.Add(-time.Second)) {
			task.Delay = runAt.Sub(time.Now())
			log.Println("DEBUG kewpie", queueName, "Republishing task", task)
			if err := this.Publish(ctx, queueName, task); err != nil {
				log.Println("ERROR kewpie", queueName, "Error republishing task", task)
			}
			this.deleteMessage(queueName, message)
			continue
		}
		requeue, err := handler.Handle(task)
		log.Println("INFO kewpie", queueName, "Task completed on queue", queueName, err, requeue)
		log.Println("DEBUG kewpie", queueName, "Task completed on queue", queueName, task, err, requeue)

		delete := true

		if err != nil {
			log.Println("ERROR kewpie", queueName, "Task failed on queue", queueName, task, err, requeue)
			delete = !requeue
			if requeue {
				task.Attempts += 1
				if !noExpBackoff {
					task.Delay, err = util.CalcBackoff(attempts)
					if err != nil {
						log.Println("ERROR kewpie", queueName, "Failed to calc backoff", queueName, task, err)
						continue
					}
				}
				this.Publish(ctx, queueName, task)
				delete = true
			}
		}

		if delete {
			this.deleteMessage(queueName, message)
		}
	}

	return this.Subscribe(ctx, queueName, handler)
}

func (this *Sqs) Init(queues []string) (err error) {
	this.urls = make(map[string]string)

	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("ap-southeast-2"),
	}))

	// Create the service's client with the session.
	this.svc = sqs.New(sess)

	params := sqs.ListQueuesInput{}
	response, err := this.svc.ListQueues(&params)
	if err != nil {
		return err
	}

	available := make(map[string]bool)
	for _, name := range queues {
		available[name] = true
	}

	for _, queueName := range response.QueueUrls {
		name := path.Base(*queueName)
		if available[name] {
			this.urls[name] = *queueName
		}
	}

	for name, _ := range available {
		if this.urls[name] != "" {
			continue
		}
		var creationParams sqs.CreateQueueInput
		creationParams.SetQueueName(name)

		twenty := "20"
		fourteenDays := "1209600"

		creationParams.SetAttributes(map[string]*string{
			"ReceiveMessageWaitTimeSeconds": &twenty,
			"MessageRetentionPeriod":        &fourteenDays,
		})

		result, err := this.svc.CreateQueue(&creationParams)
		if err != nil {
			return err
		}

		this.urls[name] = *result.QueueUrl
	}
	return
}

func (this Sqs) deleteMessage(queueName string, message *sqs.Message) error {
	url := this.urls[queueName]

	delParams := sqs.DeleteMessageInput{
		QueueUrl:      &url,
		ReceiptHandle: message.ReceiptHandle,
	}
	log.Println("DEBUG kewpie", queueName, "Deleting with", delParams)
	_, err := this.svc.DeleteMessage(&delParams)
	if err != nil {
		log.Println("ERROR kewpie", queueName, "Error deleting message!", err)
	}
	return err
}

func (this *Sqs) Disconnect() error {
	this.closed = true
	return nil
}