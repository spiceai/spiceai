package util

import (
	"strconv"
)

func ParseTime(str string) (int64, error) {
	ts, err := strconv.ParseInt(str, 10, 64)
	if err != nil {
		return -1, err
	}

	return ts, nil
}
