package util

import (
	"strconv"
	"time"
)

func ParseTime(str string) (int64, error) {
	ts, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return -1, err
	}

	return ts, nil
}

func NumIntervals(period time.Duration, granularity time.Duration) int64 {
	return int64(period / granularity)
}
