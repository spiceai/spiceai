package time

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	spice_json "github.com/spiceai/spiceai/pkg/json"
)

type Time struct {
	Integer *int64
	String  *string
}

func ParseTime(str string, format string) (time.Time, error) {
	if format == "" {
		if ts, err := strconv.ParseInt(str, 10, 64); err == nil {
			return time.Unix(ts, 0).UTC(), nil
		}
		if t, err := time.Parse(time.RFC3339, str); err == nil {
			return t.UTC(), nil
		}
		return time.Time{}, fmt.Errorf("incorrectly formatted time '%s', expected unix timestamp or rfc3339", str)
	}

	if strings.EqualFold(format, "rfc3339") {
		t, err := time.Parse(time.RFC3339, str)
		if err != nil {
			return time.Time{}, fmt.Errorf("incorrectly formatted time '%s': %s", str, err.Error())
		}
		return t.UTC(), nil
	}

	if strings.EqualFold(format, "iso8601") {
		t, err := time.Parse("2006-01-02T15:04:05-0700", str)
		if err != nil {
			return time.Time{}, fmt.Errorf("incorrectly formatted time '%s': %s", str, err.Error())
		}
		return t.UTC(), nil
	}

	if strings.EqualFold(format, "rfc1123z") {
		t, err := time.Parse(time.RFC1123Z, str)
		if err != nil {
			return time.Time{}, fmt.Errorf("incorrectly formatted time '%s': %s", str, err.Error())
		}
		return t.UTC(), nil
	}

	if strings.EqualFold(format, "rfc822z") {
		t, err := time.Parse(time.RFC822Z, str)
		if err != nil {
			return time.Time{}, fmt.Errorf("incorrectly formatted time '%s': %s", str, err.Error())
		}
		return t.UTC(), nil
	}

	t, err := time.Parse(format, str)
	if err != nil {
		return time.Time{}, fmt.Errorf("incorrectly formatted time '%s' expecting format '%s': %s", str, format, err.Error())
	}

	return t.UTC(), nil
}

func NumIntervals(period time.Duration, granularity time.Duration) int64 {
	return int64(math.Ceil(float64(period) / float64(granularity)))
}

func (x *Time) UnmarshalJSON(data []byte) error {
	err := spice_json.UnmarshalUnion(data, &x.Integer, &x.String, nil)
	if err != nil {
		return err
	}
	return nil
}

func (x *Time) MarshalJSON() ([]byte, error) {
	return spice_json.MarshalUnion(x.Integer, x.String, nil)
}
