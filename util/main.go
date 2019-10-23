package util

import (
	"time"
)

const TWENTY_SECONDS = 20 * time.Second
const BACKOFF_INTERVAL = time.Duration(TWENTY_SECONDS)
const BACKOFF_CAP = time.Duration(6 * time.Hour)

func CalcBackoff(attempts int) (time.Duration, error) {
	backoffDelay := BACKOFF_INTERVAL

	for i := 1; i < attempts; i++ {
		backoffDelay = backoffDelay * 2
		if backoffDelay > BACKOFF_CAP {
			return BACKOFF_CAP, nil
		}
	}

	return backoffDelay, nil
}
