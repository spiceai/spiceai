package util

import (
	"math"
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
	return int64(math.Ceil(float64(period) / float64(granularity)))
}
