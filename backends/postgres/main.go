package postgres

import (
	"context"
	"database/sql"
	"log"
	"os"
	"strings"
	"time"

	"github.com/davidbanham/kewpie_go/v3/types"
	"github.com/davidbanham/kewpie_go/v3/util"
	"github.com/davidbanham/required_env"
	_ "github.com/lib/pq"
	uuid "github.com/satori/go.uuid"
)

type Querier interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row
}

type Postgres struct {
	db     *sql.DB
	closed bool
}

func (this Postgres) Publish(ctx context.Context, queueName string, payload *types.Task) error {
	log.Println("DEBUG kewpie", queueName, "Publishing task", payload)

	if ctx.Value("tx") == nil {
		ctx = context.WithValue(ctx, "tx", this.db)
	}

	db := ctx.Value("tx").(Querier)

	id := uuid.NewV4().String()
	tableName := nameToTable(queueName)

	if _, err := db.ExecContext(ctx, "INSERT INTO "+tableName+" (id, body, delay, run_at, no_exp_backoff, attempts, tags) VALUES ($1, $2, $3, $4, $5, $6, $7)",
		id,
		payload.Body,
		payload.Delay,
		payload.RunAt,
		payload.NoExpBackoff,
		payload.Attempts,
		payload.Tags,
	); err != nil {
		return err
	}

	payload.ID = id

	return nil
}

func (this *Postgres) Pop(ctx context.Context, queueName string, handler types.Handler) error {
	cancelled := false

	go func() {
		for {
			select {
			case <-ctx.Done():
				cancelled = true
				return
			}
		}
	}()

	if this.closed {
		return types.ConnectionClosed
	}

	if ctx.Value("tx") == nil {
		tx, err := this.db.BeginTx(ctx, nil)
		if err != nil {
			if err == context.Canceled {
				return types.SubscriptionCancelled
			}
			return err
		}
		ctx = context.WithValue(ctx, "tx", tx)
		defer (func() {
			if err := tx.Commit(); err != nil {
				log.Println("ERROR committing transaction", err)
				this.closed = true
			}
		})()
	}

	db := ctx.Value("tx").(Querier)

	tableName := nameToTable(queueName)

	for {
		if cancelled {
			return types.SubscriptionCancelled
		}

		if this.closed {
			return types.ConnectionClosed
		}

		row := db.QueryRowContext(ctx, `DELETE FROM `+tableName+`
WHERE id = (
  SELECT id FROM `+tableName+`
	WHERE run_at < NOW()
  ORDER BY created_at, attempts ASC
  FOR UPDATE SKIP LOCKED 
  LIMIT 1
)
RETURNING id, body, delay, run_at, no_exp_backoff, attempts, tags`)

		task := types.Task{}
		if err := row.Scan(&task.ID, &task.Body, &task.Delay, &task.RunAt, &task.NoExpBackoff, &task.Attempts, &task.Tags); err != nil {
			if err == sql.ErrNoRows {
				time.Sleep(1 * time.Second)
				if err := this.db.Ping(); err != nil {
					log.Println("ERROR postgres database ping", err.Error())
					this.closed = true
				}
				continue
				//return this.Pop(ctx, queueName, handler)
			}
			if err == context.Canceled {
				return types.SubscriptionCancelled
			}
			return err
		}

		log.Println("INFO kewpie Received task from queue", queueName)
		log.Println("DEBUG kewpie Received task from queue", queueName, task)

		requeue, err := handler.Handle(task)

		log.Println("INFO kewpie", queueName, "Task completed on queue", queueName, err, requeue)
		log.Println("DEBUG kewpie", queueName, "Task completed on queue", queueName, task, err, requeue)

		if err != nil {
			log.Println("ERROR kewpie", queueName, "Task failed on queue", queueName, task, err, requeue)
			if requeue {
				task.Attempts += 1
				if !task.NoExpBackoff {
					task.Delay = util.CalcBackoff(task.Attempts + 1)
				}
				if err := this.Publish(ctx, queueName, &task); err != nil {
					return err
				}
			}
		}

		return nil
	}

	return nil
}

func (this Postgres) Subscribe(ctx context.Context, queueName string, handler types.Handler) error {
	for {
		if this.closed {
			return types.ConnectionClosed
		}

		if err := this.Pop(ctx, queueName, handler); err != nil {
			return err
		}
	}
}

func (this *Postgres) PassConnection(connection *sql.DB) {
	this.db = connection
}

func (this *Postgres) Connect() (*sql.DB, error) {
	return sql.Open("postgres", os.Getenv("DB_URI"))
}

func (this *Postgres) Init(queues []string) error {
	required_env.Ensure(map[string]string{
		"DB_URI": "",
	})

	if this.db == nil {
		db, err := this.Connect()
		if err != nil {
			return err
		}
		this.db = db
	}

	for _, name := range queues {
		tableName := nameToTable(name)
		if _, err := this.db.Exec(`CREATE TABLE IF NOT EXISTS ` + tableName + ` (
id UUID PRIMARY KEY,
body TEXT NOT NULL DEFAULT '',
delay BIGINT NOT NULL DEFAULT 0,
run_at TIMESTAMPTZ default NOW(),
created_at TIMESTAMPTZ default NOW(),
no_exp_backoff BOOL NOT NULL DEFAULT false,
attempts int NOT NULL DEFAULT 0,
tags JSONB NOT NULL DEFAULT '{}'::jsonb
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

func (this Postgres) Purge(ctx context.Context, queueName string) error {
	return this.PurgeMatching(ctx, queueName, "*")
}

func (this Postgres) PurgeMatching(ctx context.Context, queueName, substr string) error {
	if this.closed {
		return types.ConnectionClosed
	}

	if ctx.Value("tx") == nil {
		ctx = context.WithValue(ctx, "tx", this.db)
	}

	db := ctx.Value("tx").(Querier)

	tableName := nameToTable(queueName)

	var res sql.Result
	var err error
	if substr == "*" {
		res, err = db.ExecContext(ctx, `DELETE FROM `+tableName)
	} else {
		res, err = db.ExecContext(ctx, `DELETE FROM `+tableName+` WHERE body LIKE '%' || $1 || '%'`, substr)
	}
	if err != nil {
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return err
	}

	log.Printf("INFO kewpie Purged %d tasks from %s\n", rows, queueName)

	return nil
}

func (this Postgres) Healthy(ctx context.Context) error {
	return this.db.Ping()
}

func nameToTable(name string) string {
	name = strings.Replace(name, " ", "_", -1) // no spaces
	name = strings.Replace(name, "-", "_", -1) // no dashes
	name = strings.ToLower(name)               // lower case
	return "kewpie_" + name
}
