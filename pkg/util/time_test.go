package util

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTime(t *testing.T) {
	t.Run("ParseTime()", testParseTimeFunc())
	t.Run("NumIntervals()", testNumIntervalsFunc())
}

func BenchmarkTime(b *testing.B) {
	b.Run("ParseTime()", benchParseTimeFunc())
}

// Tests "ParseTime()"
func testParseTimeFunc() func(*testing.T) {
	return func(t *testing.T) {
		var expectedTime int64 = 1605312000
		actualTime, err := ParseTime("1605312000")

		assert.NoError(t, err, "ParseTime() failed")
		assert.Equal(t, expectedTime, actualTime, "ParseTime() was incorrect")

		_, err = ParseTime("invalid time")
		assert.Error(t, err, "ParseTime() did not return err")
	}
}

// Benchmarks "ParseTime()"
func benchParseTimeFunc() func(*testing.B) {
	return func(b *testing.B) {
		for i := 0; i < 100; i++ {
			_, err := ParseTime("1605312000")
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
