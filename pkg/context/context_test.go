package context_test

import (
	"os"
	"testing"

	"github.com/spiceai/spice/pkg/context"
	"github.com/stretchr/testify/assert"
)

func TestContext(t *testing.T) {
	t.Run("CurrentContext() - Context is set correctly", testCurrentContext())
}

// Tests CurrentContext() inferring the correct context from the environment
func testCurrentContext() func(*testing.T) {
	return func(t *testing.T) {
		orig := os.Getenv("SPICE_ENVIRONMENT")
		t.Cleanup(func() { os.Setenv("SPICE_ENVIRONMENT", orig) })

		os.Setenv("SPICE_ENVIRONMENT", "docker")
		assert.EqualValues(t, context.Docker, context.CurrentContext())
		context.SetContext(context.Undefined)

		os.Setenv("SPICE_ENVIRONMENT", "")
		assert.EqualValues(t, context.BareMetal, context.CurrentContext())
	}
}
