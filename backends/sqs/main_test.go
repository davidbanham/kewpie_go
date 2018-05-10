package sqs

import (
	"testing"
	"time"
)

func TestCalcDelay(t *testing.T) {
	// It should round long delays to 15 minutes
	originalLongDelay := time.Duration(45 * time.Minute)
	longDelay := roundTo15(originalLongDelay)
	if longDelay != 900*time.Second {
		t.Log("longDelay is", longDelay)
		t.Fatal("longDelay is not 15 minutes")
	}

	// It should not round delays shorter than 15 minutes
	originalShortDelay := time.Duration(6 * time.Minute)
	shortDelay := roundTo15(originalShortDelay)
	if shortDelay != originalShortDelay {
		t.Log("shortDelay is", shortDelay, originalShortDelay)
		t.Fatal("shortDelay was changed")
	}
}
