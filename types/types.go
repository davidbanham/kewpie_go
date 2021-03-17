package types

import (
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"time"

	jwt "github.com/dgrijalva/jwt-go/v4"
	uuid "github.com/satori/go.uuid"
)

type Task struct {
	ID           string        `json:"id"`
	Body         string        `json:"body"`
	Delay        time.Duration `json:"delay"` // Delay overrides RunAt
	RunAt        time.Time     `json:"run_at"`
	NoExpBackoff bool          `json:"no_exp_backoff"`
	Attempts     int           `json:"attempts"`
	Tags         Tags          `json:"tags"`
	QueueName    string        `json:"queue_name"`
}

type Tags map[string]string

func (tags Tags) Value() (driver.Value, error) {
	return json.Marshal(tags)
}

func (tags *Tags) Scan(value interface{}) error {
	b, ok := value.([]byte)
	if !ok {
		return errors.New("type assertion to []byte failed")
	}

	return json.Unmarshal(b, &tags)
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

func (t *Task) FromHTTP(r *http.Request) error {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(body, &t); err != nil {
		return err
	}

	parsed, _ := strconv.Atoi(r.Header.Get("X-CloudTasks-TaskRetryCount"))
	t.Attempts = parsed

	if t.QueueName == "" {
		t.QueueName = r.Header.Get("X-CloudTasks-QueueName")
	}

	return nil
}

func (t *Task) Sign(secret string) error {
	uniq := uuid.NewV4().String()
	t.Tags["kewpie_auth_claim"] = uniq

	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"auth": uniq,
	})

	tokenString, err := token.SignedString([]byte(secret))
	if err != nil {
		return err
	}

	t.Tags["kewpie_token"] = tokenString
	return nil
}

func (task *Task) VerifySignature(secret string) error {
	token, err := jwt.Parse(task.Tags["token"], func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return "", fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}

		return []byte(secret), nil
	})
	if err != nil {
		return err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		if claims["auth"] != task.Tags["kewpie_auth_claim"] {
			return fmt.Errorf("Invalid token")
		}
	} else {
		return fmt.Errorf("Error decoding token claims")
	}

	return nil
}

type Handler interface {
	Handle(Task) (bool, error)
}

var QueueNotFound = errors.New("I don't know any queue by that name")
var ConnectionClosed = errors.New("The connection to the backend is closed")
var SubscriptionCancelled = errors.New("This subscription has been cancelled")
var NotImplemented = errors.New("This method is not implemented on this backend")
var UnknownBackend = errors.New("I have never heard of that backend")
