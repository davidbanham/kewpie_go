package kewpie

import (
	"encoding/json"
	"errors"
	"log"
	"path"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var QueueNotFound = errors.New("I don't know any queue by that name")

const TWENTY_SECONDS = int64(20)
const FIFTEEN_MINUTES = (15 * time.Minute)
const BACKOFF_INTERVAL = TWENTY_SECONDS
const BACKOFF_CAP = int64(6 * time.Hour)

type Kewpie struct {
	url  string
	urls map[string]string
	svc  *sqs.SQS
}

type Task struct {
	Body         string
	Delay        time.Duration
	NoExpBackoff bool
	Attempts     int
}

func (t Task) Unmarshal(res interface{}) (err error) {
	err = json.Unmarshal([]byte(t.Body), res)
	return
}

func (t *Task) Marshal(source interface{}) (err error) {
	byteArr, err := json.Marshal(source)
	t.Body = string(byteArr)
	return
}

type Handler interface {
	Handle(Task) (bool, error)
}

func roundTo15(orig time.Duration) time.Duration {
	if orig > FIFTEEN_MINUTES {
		return time.Duration(FIFTEEN_MINUTES)
	}
	return orig
}

func (this Kewpie) Publish(queueName string, payload Task) (err error) {
	log.Println("DEBUG kewpie", queueName, "Publishing task", payload)
	url := this.urls[queueName]
	if url == "" {
		err = QueueNotFound
		return
	}

	runAt := time.Now().Add(payload.Delay)

	delay := int64(roundTo15(payload.Delay).Seconds())

	noExpBackoff := "false"
	if payload.NoExpBackoff {
		noExpBackoff = "true"
	}

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

func (this Kewpie) Subscribe(queueName string, handler Handler) (err error) {
	url := this.urls[queueName]
	if url == "" {
		err = QueueNotFound
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
		task := Task{
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
			if err := this.Publish(queueName, task); err != nil {
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
			if !delete && !noExpBackoff {
				task.Delay, err = calcBackoff(attempts)
				if err != nil {
					log.Println("ERROR kewpie", queueName, "Failed to calc backoff", queueName, task, err)
					continue
				}
				this.Publish(queueName, task)
				delete = true
			}
		}

		if delete {
			this.deleteMessage(queueName, message)
		}
	}
	return this.Subscribe(queueName, handler)
}

func (this Kewpie) deleteMessage(queueName string, message *sqs.Message) error {
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

func (this *Kewpie) Connect(url string, queues []string) (err error) {
	this.url = url
	this.urls = map[string]string{}

	switch url {
	case "test":
		return
	case "sqs":
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
	}
	return
}

func calcBackoff(attempts int) (delay time.Duration, err error) {
	backoffDelay := BACKOFF_INTERVAL

	for i := 1; i < attempts; i++ {
		backoffDelay = backoffDelay * 2
		if backoffDelay > BACKOFF_CAP {
			backoffDelay = BACKOFF_CAP
			break
		}
	}

	backoffDelayDuration, err := time.ParseDuration(strconv.Itoa(int(backoffDelay)) + "s")

	if err != nil {
		return delay, err
	}
	runAt := time.Now().Add(backoffDelayDuration)
	delay = time.Now().Sub(runAt)
	return
}
