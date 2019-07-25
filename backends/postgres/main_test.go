package postgres

import (
	"context"
	"testing"
	"time"

	"github.com/davidbanham/kewpie_go/types"
	uuid "github.com/satori/go.uuid"
	"github.com/stretchr/testify/assert"
)

type supDawg struct {
	Sup string
}

type testHandler struct {
	fired      bool
	supText    string
	handleFunc func(types.Task) (bool, error)
}

func (h *testHandler) Handle(t types.Task) (bool, error) {
	return h.handleFunc(t)
}

func TestOrder(t *testing.T) {
	ctx := context.Background()
	pg := Postgres{}
	queueName := uuid.NewV4().String()
	assert.Nil(t, pg.Init([]string{queueName}))

	uniq1 := uuid.NewV4().String()
	uniq2 := uuid.NewV4().String()

	pubTask1 := types.Task{}
	err := pubTask1.Marshal(supDawg{
		Sup: uniq1,
	})
	if err != nil {
		t.Fatal("Err in marshaling")
	}
	pubTask2 := types.Task{}
	err = pubTask2.Marshal(supDawg{
		Sup: uniq2,
	})
	if err != nil {
		t.Fatal("Err in marshaling")
	}
	assert.Nil(t, pg.Publish(ctx, queueName, &pubTask1))
	assert.Nil(t, pg.Publish(ctx, queueName, &pubTask2))

	hit := false
	handler := &testHandler{
		handleFunc: func(task types.Task) (requeue bool, err error) {
			if hit {
				return false, nil
			}

			monty := supDawg{}
			task.Unmarshal(&monty)

			hit = true

			if monty.Sup != uniq1 {
				t.Log("Wasn't the first task")
				t.Fail()
			}
			return false, nil
		},
	}
	go pg.Subscribe(ctx, queueName, handler)
	time.Sleep(1 * time.Second)
	assert.True(t, hit)
}
