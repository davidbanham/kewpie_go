package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBackoffCap(t *testing.T) {
	delay := CalcBackoff(1000)
	assert.Equal(t, BACKOFF_CAP, delay)
}

func TestBackoff(t *testing.T) {
	expected, err := time.ParseDuration("40s")
	assert.Nil(t, err)
	assert.Equal(t, expected, CalcBackoff(2))

	expected, err = time.ParseDuration("80s")
	assert.Nil(t, err)
	assert.Equal(t, expected, CalcBackoff(3))
}
