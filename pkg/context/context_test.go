package context_test

import (
	"testing"

	"github.com/spiceai/spiceai/pkg/context"
	"github.com/spiceai/spiceai/pkg/context/metal"
	"github.com/stretchr/testify/assert"
)

func TestContext(t *testing.T) {
	t.Run("CurrentContext() - Context is set correctly", testCurrentContext())
	t.Run("SetDefaultContext() - Context is set correctly", testSetDefaultContext())
}

// Tests CurrentContext() inferring the correct context from the environment
func testCurrentContext() func(*testing.T) {
	return func(t *testing.T) {
		assert.IsType(t, &metal.MetalContext{}, context.CurrentContext())
	}
}

// Tests SetDefaultContext()
func testSetDefaultContext() func(*testing.T) {
	return func(t *testing.T) {
		// Ensure no context is set
		context.SetContext(nil)

		err := context.SetDefaultContext()
		assert.NoError(t, err)

		assert.IsType(t, &metal.MetalContext{}, context.CurrentContext())
	}
}
