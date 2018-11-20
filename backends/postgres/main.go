package postgres

import (
	"context"
	"database/sql"
	"log"
	"os"
	"strings"
	"time"

	"github.com/davidbanham/kewpie_go/types"
	"github.com/davidbanham/kewpie_go/util"
	"github.com/davidbanham/required_env"
	_ "github.com/lib/pq"
	uuid "github.com/satori/go.uuid"
)

type Querier interface {
	BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error)
}

type Postgres struct {
	db     *sql.DB
	closed bool
}

func (this Postgres) Publish(ctx context.Context, queueName string, payload types.Task) error {
	if ctx.Value("tx") == nil {
		ctx = context.WithValue(ctx, "tx", this.db)
	}

	db := ctx.Value("tx").(Querier)

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	payload.RunAt = time.Now().Add(payload.Delay)

	id := uuid.NewV4().String()
	tableName := nameToTable(queueName)

	if _, err := tx.ExecContext(ctx, "INSERT INTO "+tableName+" (id, body, delay, run_at, no_exp_backoff, attempts) VALUES ($1, $2, $3, $4, $5, $6)",
		id,
		payload.Body,
		payload.Delay,
		payload.RunAt,
		payload.NoExpBackoff,
		payload.Attempts,
	); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func (this Postgres) Subscribe(ctx context.Context, queueName string, handler types.Handler) error {
	if this.closed {
		return nil
	}

	var tx *sql.Tx

	if ctx.Value("tx") == nil {
		var err error
		tx, err = this.db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
	} else {
		db := ctx.Value("tx").(Querier)
		var err error
		tx, err = db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
	}

	tableName := nameToTable(queueName)

	row := tx.QueryRowContext(ctx, `DELETE FROM `+tableName+`
WHERE id = (
  SELECT id FROM `+tableName+`
	WHERE run_at < NOW()
  ORDER BY created_at, attempts ASC
  FOR UPDATE SKIP LOCKED 
  LIMIT 1
)
RETURNING id, body, delay, run_at, no_exp_backoff, attempts`)

	task := types.Task{}
	var id string
	if err := row.Scan(&id, &task.Body, &task.Delay, &task.RunAt, &task.NoExpBackoff, &task.Attempts); err != nil {
		if err == sql.ErrNoRows {
			time.Sleep(1 * time.Second)
			return this.Subscribe(ctx, queueName, handler)
		}
		return err
	}

	requeue, err := handler.Handle(task)

	log.Println("INFO kewpie", queueName, "Task completed on queue", queueName, err, requeue)

	if err != nil {
		if requeue {
			task.Attempts += 1
			if !task.NoExpBackoff {
				task.Delay, err = util.CalcBackoff(task.Attempts + 1)
				if err != nil {
					log.Println("ERROR kewpie", queueName, "Failed to calc backoff", queueName, task, err)
					return err
				}
			}
			subCtx := context.WithValue(ctx, "tx", this.db)
			if err := this.Publish(subCtx, queueName, task); err != nil {
				return err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return this.Subscribe(ctx, queueName, handler)
}

func (this *Postgres) Init(queues []string) error {
	required_env.Ensure(map[string]string{
		"DB_URI": "",
	})

	dbURI := os.Getenv("DB_URI")
	db, err := sql.Open("postgres", dbURI)
	if err != nil {
		return err
	}

	this.db = db

	for _, name := range queues {
		tableName := nameToTable(name)
		if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS ` + tableName + ` (
id UUID PRIMARY KEY,
body TEXT NOT NULL DEFAULT '',
delay INTEGER NOT NULL DEFAULT 0,
run_at TIMESTAMPTZ default NOW(),
created_at TIMESTAMPTZ default NOW(),
no_exp_backoff BOOL NOT NULL DEFAULT false,
attempts int NOT NULL DEFAULT 0
)`); err != nil {
			return err
		}
	}

	return nil
}

func (this *Postgres) Disconnect() error {
	this.closed = true
	return this.db.Close()
}

func nameToTable(name string) string {
	name = strings.Replace(name, " ", "_", -1) // no spaces
	name = strings.Replace(name, "-", "_", -1) // no dashes
	name = strings.ToLower(name)               // lower case
	return "kewpie_" + name
}
