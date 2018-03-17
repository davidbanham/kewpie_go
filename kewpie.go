package kewpie

import (
	"encoding/json"
	"errors"
	"log"
	"path"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
)

var QueueNotFound = errors.New("I don't know any queue by that name")

const TWENTY_SECONDS = int64(20)
const FIFTEEN_MINUTES = (15 * time.Minute)

type Task struct {
	Body  string
	Delay time.Duration
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

var svc *sqs.SQS

type Handler interface {
	Handle(Task) (bool, error)
}

var urls = make(map[string]string)

func roundTo15(orig time.Duration) time.Duration {
	if orig > FIFTEEN_MINUTES {
		return time.Duration(FIFTEEN_MINUTES)
	}
	return orig
}

func Publish(queueName string, payload Task) (err error) {
	log.Println("DEBUG kewpie", queueName, "Publishing task", payload)
	url := urls[queueName]
	if url == "" {
		err = QueueNotFound
		return
	}

	runAt := time.Now().Add(payload.Delay)

	delay := int64(roundTo15(payload.Delay).Seconds())

	message := sqs.SendMessageInput{
		DelaySeconds: &delay,
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"RunAt": &sqs.MessageAttributeValue{
				DataType:    aws.String("String"),
				StringValue: aws.String(runAt.UTC().Format(time.RFC3339)),
			},
		},
		MessageBody: &payload.Body,
		QueueUrl:    &url,
	}
	_, err = svc.SendMessage(&message)
	return
}

func Subscribe(queueName string, handler Handler) (err error) {
	url := urls[queueName]
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
		},
	}
	response, err := svc.ReceiveMessage(&params)
	if err != nil {
		log.Println("ERROR kewpie", queueName, "Error recieving message from queue", queueName, err)
		return
	}
	for _, message := range response.Messages {
		task := Task{
			Body: *message.Body,
		}
		log.Println("INFO kewpie Received task from queue", queueName, task)
		log.Println("DEBUG kewpie Received task from queue", queueName)

		runAtPtr := message.MessageAttributes["RunAt"]

		if runAtPtr == nil {
			log.Println("ERROR kewpie", queueName, "RunAt was nil", message)
			continue
		}

		runAtString := *runAtPtr.StringValue

		runAt, err := time.Parse(time.RFC3339, runAtString)
		if err != nil {
			log.Println("ERROR kewpie", queueName, "Error decoding runAt from message", message)
			continue
		}
		now := time.Now()
		// Knock runAt back 1s to avoid off-by-one error on comparison
		if now.Before(runAt.Add(-time.Second)) {
			task.Delay = runAt.Sub(time.Now())
			log.Println("DEBUG kewpie", queueName, "Republishing task", task)
			Publish(queueName, task)
			deleteMessage(queueName, message)
			continue
		}
		requeue, err := handler.Handle(task)
		log.Println("INFO kewpie", queueName, "Task completed on queue", queueName, err, requeue)
		log.Println("DEBUG kewpie", queueName, "Task completed on queue", queueName, task, err, requeue)
		delete := true
		if err != nil {
			delete = !requeue
		}
		if delete {
			deleteMessage(queueName, message)
		}
	}
	return Subscribe(queueName, handler)
}

func deleteMessage(queueName string, message *sqs.Message) error {
	url := urls[queueName]

	delParams := sqs.DeleteMessageInput{
		QueueUrl:      &url,
		ReceiptHandle: message.ReceiptHandle,
	}
	log.Println("DEBUG kewpie", queueName, "Deleting with", delParams)
	_, err := svc.DeleteMessage(&delParams)
	if err != nil {
		log.Println("ERROR kewpie", queueName, "Error deleting message!", err)
	}
	return err
}

// FIXME This is currently a gross singleton. It should probably return a Kewpie type that has pub/sub methods.
// It'll probably work fine as is, but won't support connecting to multiple URLs (which it currently ignores anyway since we're SQS only at this point)
func Connect(url string, queues []string) (err error) {
	sess := session.Must(session.NewSession(&aws.Config{
		Region: aws.String("ap-southeast-2"),
	}))

	// Create the service's client with the session.
	svc = sqs.New(sess)

	params := sqs.ListQueuesInput{}
	response, err := svc.ListQueues(&params)
	if err != nil {
		return
	}

	available := make(map[string]bool)
	for _, name := range queues {
		available[name] = true
	}

	for _, queueName := range response.QueueUrls {
		name := path.Base(*queueName)
		if available[name] {
			urls[name] = *queueName
		}
	}

	for name, _ := range available {
		if urls[name] != "" {
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

		result, err := svc.CreateQueue(&creationParams)
		if err != nil {
			return err
		}

		urls[name] = *result.QueueUrl
	}

	return
}
