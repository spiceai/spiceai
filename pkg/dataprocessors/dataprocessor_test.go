package dataprocessors

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewDataProcessor(t *testing.T) {
	t.Run("NewDataProcessor() - Invalid processor", testNewDataProcessorUnknownFunc())
}

func testNewDataProcessorUnknownFunc() func(*testing.T) {
	return func(t *testing.T) {
		_, err := NewDataProcessor("does-not-exist")
		assert.Error(t, err)
	}
}
