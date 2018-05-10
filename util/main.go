package util

import (
	"strconv"
	"time"
)

const TWENTY_SECONDS = int64(20)
const BACKOFF_INTERVAL = TWENTY_SECONDS
const BACKOFF_CAP = int64(6 * time.Hour)

func CalcBackoff(attempts int) (delay time.Duration, err error) {
	backoffDelay := BACKOFF_INTERVAL

	for i := 1; i < attempts; i++ {
		backoffDelay = backoffDelay * 2
		if backoffDelay > BACKOFF_CAP {
			backoffDelay = BACKOFF_CAP
			break
		}
	}

	backoffDelayDuration, err := time.ParseDuration(strconv.Itoa(int(backoffDelay)) + "s")

	if err != nil {
		return delay, err
	}
	runAt := time.Now().Add(backoffDelayDuration)
	delay = time.Now().Sub(runAt)
	return
}
