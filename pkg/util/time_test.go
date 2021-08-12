package util

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTime(t *testing.T) {
	t.Run("ParseTime()", testParseTimeFunc())
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
