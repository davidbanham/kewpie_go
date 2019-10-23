package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBackoffCap(t *testing.T) {
	delay, err := CalcBackoff(1000)
	assert.Nil(t, err)
	assert.Equal(t, BACKOFF_CAP, delay)
}

func TestBackoff(t *testing.T) {
	expected, err := time.ParseDuration("40s")
	assert.Nil(t, err)
	delay, err := CalcBackoff(2)
	assert.Nil(t, err)
	assert.Equal(t, expected, delay)

	expected, err = time.ParseDuration("80s")
	assert.Nil(t, err)
	delay, err = CalcBackoff(3)
	assert.Nil(t, err)
	assert.Equal(t, expected, delay)
}
