package googlecloudtasks

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	duration "github.com/golang/protobuf/ptypes/duration"
	"google.golang.org/api/iterator"
	taskspb "google.golang.org/genproto/googleapis/cloud/tasks/v2"

	"github.com/davidbanham/kewpie_go/v3/types"
	"github.com/davidbanham/required_env"
	"github.com/golang/protobuf/ptypes"
)

type CloudTasks struct {
	client             *cloudtasks.Client
	paths              map[string]string
	closed             bool
	publishConcurrency int
}

func sanitise(queueName string) string {
	return strings.ReplaceAll(queueName, "_", "-")
}

func (this *CloudTasks) Init(queues []string) error {
	this.paths = map[string]string{}

	ctx := context.Background()
	client, err := cloudtasks.NewClient(ctx)
	if err != nil {
		return err
	}

	this.client = client

	required_env.Ensure(map[string]string{
		"GOOGLE_PROJECT_ID":                      "",
		"GOOGLE_CLOUD_LOCATION_ID":               "",
		"GOOGLE_CLOUD_TASKS_PUBLISH_CONCURRENCY": "10",
	})

	projectID := os.Getenv("GOOGLE_PROJECT_ID")
	locationID := os.Getenv("GOOGLE_CLOUD_LOCATION_ID")

	publishConcurrency, err := strconv.Atoi(os.Getenv("GOOGLE_CLOUD_TASKS_PUBLISH_CONCURRENCY"))
	if err != nil {
		log.Fatal("Invalid concurrency integer provided", err)
	}

	this.publishConcurrency = publishConcurrency

	for _, queueName := range queues {
		name := fmt.Sprintf("projects/%s/locations/%s/queues/%s", projectID, locationID, sanitise(queueName))

		go (func() {
			getReq := taskspb.GetQueueRequest{
				Name: name,
			}
			if _, err := this.client.GetQueue(ctx, &getReq); err != nil {
				req := taskspb.UpdateQueueRequest{
					Queue: &taskspb.Queue{
						Name: name,
					},
				}
				if _, err := this.client.UpdateQueue(ctx, &req); err != nil {
					log.Fatal(err)
				}
			}
		})()

		this.paths[sanitise(queueName)] = name
	}

	return nil
}

func (this CloudTasks) Pop(ctx context.Context, queueName string, handler types.Handler) error {
	return types.NotImplemented
}

func (this CloudTasks) Suck(ctx context.Context, queueName string, handler types.Handler) error {
	return types.NotImplemented
}

func (this CloudTasks) Subscribe(ctx context.Context, queueName string, handler types.Handler) error {
	return types.NotImplemented
}

func (this CloudTasks) Publish(ctx context.Context, queueName string, payload *types.Task) error {
	if this.closed {
		return types.ConnectionClosed
	}

	runAt := payload.RunAt

	horizon := time.Now().Add(25 * 24 * time.Hour) // Cloud Tasks max is 30 days, use 25 to give plenty of leeway for leap seconds, etc.

	if runAt.After(horizon) {
		runAt = horizon
	}

	ts, err := ptypes.TimestampProto(runAt)
	if err != nil {
		return err
	}

	targetURL := payload.Tags["handler_url"]

	req := &taskspb.CreateTaskRequest{
		Parent: this.paths[sanitise(queueName)],
		Task: &taskspb.Task{
			ScheduleTime:     ts,
			DispatchDeadline: &duration.Duration{Seconds: int64(payload.Timeout.Seconds())},
			MessageType: &taskspb.Task_HttpRequest{
				HttpRequest: &taskspb.HttpRequest{
					Headers: map[string]string{
						"Content-Type": "application/json",
					},
					HttpMethod: taskspb.HttpMethod_POST,
					Url:        targetURL,
				},
			},
		},
	}

	taskJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	req.Task.GetHttpRequest().Body = taskJSON

	createdTask, err := this.client.CreateTask(ctx, req)
	if err != nil {
		return fmt.Errorf("cloudtasks.CreateTask: %v", err)
	}

	payload.ID = createdTask.GetName()

	return nil
}

func (this CloudTasks) Purge(ctx context.Context, queueName string) error {
	req := &taskspb.ListTasksRequest{
		Parent: this.paths[sanitise(queueName)],
	}
	it := this.client.ListTasks(ctx, req)
	for {
		task, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return err
		}

		deleteReq := &taskspb.DeleteTaskRequest{
			Name: task.GetName(),
		}
		if err := this.client.DeleteTask(ctx, deleteReq); err != nil {
			return err
		}
	}
	return nil
}

func (this CloudTasks) PurgeMatching(ctx context.Context, queueName, taskName string) error {
	deleteReq := &taskspb.DeleteTaskRequest{
		Name: taskName,
	}
	if err := this.client.DeleteTask(ctx, deleteReq); err != nil {
		return err
	}
	return nil
}

func (this *CloudTasks) Disconnect() error {
	this.closed = true
	return this.client.Close()
}

func (this CloudTasks) Healthy(ctx context.Context) error {
	// FIXME implement this
	return nil
}

func (this CloudTasks) MaxConcurrentDrainWorkers() int {
	return this.publishConcurrency
}
