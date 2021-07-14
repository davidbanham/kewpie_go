package types

import (
	"crypto/sha256"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	jwt "github.com/dgrijalva/jwt-go/v4"
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
	Token        string        `json:"token"`
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

func (tags Tags) Get(key string) string {
	if tags == nil {
		return ""
	}
	return tags[key]
}

func (tags *Tags) Set(key, value string) {
	if *tags == nil {
		(*tags) = map[string]string{}
	}
	(*tags)[key] = value
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
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}

	if err := json.Unmarshal(body, &t); err != nil {
		if strings.Contains(err.Error(), "unexpected end of JSON input") {
			return fmt.Errorf("invalid JSON body passed to FromHTTP - %s - %s - %w", string(body), r.Form, err)
		}
		return err
	}

	parsed, _ := strconv.Atoi(r.Header.Get("X-CloudTasks-TaskRetryCount"))
	t.Attempts = parsed

	if t.QueueName == "" {
		t.QueueName = r.Header.Get("X-CloudTasks-QueueName")
	}

	return nil
}

func (t Task) Checksum() string {
	sum := sha256.Sum256([]byte(t.Body))
	return fmt.Sprintf("%x", sum)
}

func (t *Task) Sign(secret string) error {
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"checksum": t.Checksum(),
	})

	tokenString, err := token.SignedString([]byte(secret))
	if err != nil {
		return err
	}

	t.Token = tokenString
	return nil
}

func (task Task) VerifySignature(secret string) error {
	token, err := jwt.Parse(task.Token, func(token *jwt.Token) (interface{}, error) {
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return "", fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}

		return []byte(secret), nil
	})
	if err != nil {
		return err
	}

	if claims, ok := token.Claims.(jwt.MapClaims); ok && token.Valid {
		if claims["checksum"] != task.Checksum() {
			return fmt.Errorf("Checksum does not match. Task may have been tampered with.")
		}
	} else {
		return fmt.Errorf("Invalid token")
	}

	return nil
}

type Handler interface {
	Handle(Task) (bool, error)
}

type HTTPError struct {
	Status int
	Error  error
}

var QueueNotFound = errors.New("I don't know any queue by that name")
var ConnectionClosed = errors.New("The connection to the backend is closed")
var SubscriptionCancelled = errors.New("This subscription has been cancelled")
var NotImplemented = errors.New("This method is not implemented on this backend")
var UnknownBackend = errors.New("I have never heard of that backend")
