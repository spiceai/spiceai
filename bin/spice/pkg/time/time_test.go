package time

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTime(t *testing.T) {
	t.Run("ParseTime() - unix no format", testParseTimeFunc("1605312000", "", time.Unix(1605312000, 0).UTC()))
	t.Run("ParseTime() - rfc3339 no format", testParseTimeFunc("2009-01-01T12:59:59Z", "", time.Date(2009, 01, 01, 12, 59, 59, 0, time.UTC)))
	t.Run("ParseTime() - rfc3339 with format", testParseTimeFunc("2009-01-01T12:59:59Z", "rfc3339", time.Date(2009, 01, 01, 12, 59, 59, 0, time.UTC)))
	t.Run("ParseTime() - ISO8601", testParseTimeFunc("2016-07-25T02:22:33+0000", "iso8601", time.Date(2016, 7, 25, 2, 22, 33, 0, time.UTC)))
	t.Run("ParseTime() - rfc1123z", testParseTimeFunc("Mon, 27 Sep 2021 15:29:36 +0000", "rfc1123z", time.Date(2021, 9, 27, 15, 29, 36, 0, time.UTC)))
	t.Run("ParseTime() - rfc822z", testParseTimeFunc("27 Sep 21 15:29 +0000", "rfc822z", time.Date(2021, 9, 27, 15, 29, 0, 0, time.UTC)))
	t.Run("ParseTime() - custom format", testParseTimeFunc("2019-03-19 12:41:58+00:00", "2006-01-02 15:04:05-07:00", time.Date(2019, time.March, 19, 12, 41, 58, 0, time.UTC)))
	t.Run("ParseTime() - hex", testParseTimeFunc("618C6F9F", "hex", time.Date(2021, time.November, 11, 1, 19, 27, 0, time.UTC)))
	t.Run("ParseTime() - hex with prefix", testParseTimeFunc("0x618C6F9F", "hex", time.Date(2021, time.November, 11, 1, 19, 27, 0, time.UTC)))
	t.Run("ParseTime() - detected hex by prefix", testParseTimeFunc("0x618C6F9F", "", time.Date(2021, time.November, 11, 1, 19, 27, 0, time.UTC)))
}

func TestNumIntervals(t *testing.T) {
	t.Run("NumIntervals()", testNumIntervalsFunc())
}

func BenchmarkTime(b *testing.B) {
	b.Run("ParseTime() - unix no format", benchParseTimeFunc("1605312000", ""))
	b.Run("ParseTime() - rfc3339 no format", benchParseTimeFunc("2009-01-01T12:59:59Z", ""))
	b.Run("ParseTime() - rfc3339 format specified", benchParseTimeFunc("2009-01-01T12:59:59Z", "rfc3339"))
	b.Run("ParseTime() - iso8601", benchParseTimeFunc("2016-07-25T02:22:33+0000", "iso8601"))
	b.Run("ParseTime() - hex detected by prefix", benchParseTimeFunc("0x618C6F9F", ""))
	b.Run("ParseTime() - hex", benchParseTimeFunc("0x618C6F9F", "hex"))
}

// Tests "ParseTime() success"
func testParseTimeFunc(str string, format string, expected time.Time) func(*testing.T) {
	return func(t *testing.T) {
		actualTime, err := ParseTime(str, format)

		assert.NoError(t, err, "ParseTime() failed")
		assert.Equal(t, expected.UTC(), actualTime, "ParseTime() was incorrect")

		_, err = ParseTime("invalid time", "")
		assert.Error(t, err, "ParseTime() did not return err")
	}
}

// Benchmarks "ParseTime()"
func benchParseTimeFunc(str string, format string) func(*testing.B) {
	return func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			_, err := ParseTime(str, format)
			if err != nil {
				b.Fatal(err.Error())
			}
		}
	}
}

// Tests "NumIntervals()"
func testNumIntervalsFunc() func(*testing.T) {
	return func(t *testing.T) {
		// Exact
		actual := NumIntervals(87*time.Second, time.Second)
		assert.Equal(t, int64(87), actual)

		// Less than midpoint
		actual = NumIntervals(30*time.Second, 7*time.Second)
		assert.Equal(t, int64(5), actual)

		// Midpoint
		actual = NumIntervals(30*time.Second, 4*time.Second)
		assert.Equal(t, int64(8), actual)

		// More than Midpoint
		actual = NumIntervals(30*time.Second, 8*time.Second)
		assert.Equal(t, int64(4), actual)
	}
}
