package docker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDockerContext(t *testing.T) {
	t.Run("Name()", func(t *testing.T) {
		c := NewDockerContext()
		assert.Equal(t, "docker", c.Name())
	})

	t.Run("getDockerArgs() - production mode", func(t *testing.T) {
		c := NewDockerContext()
		expectedArgs := []string{"initial-arg1", "initial-arg2", "initial-arg3"}
		assert.Equal(t, expectedArgs, c.getDockerArgs("initial-arg1 initial-arg2  initial-arg3"))
	})

	t.Run("getDockerArgs() - development mode", func(t *testing.T) {
		c := NewDockerContext()
		if err := c.Init(true); err != nil {
			t.Fatal(err)
		}

		expectedArgs := []string{"initial-arg1", "initial-arg2", "initial-arg3", "--development"}
		assert.Equal(t, expectedArgs, c.getDockerArgs("initial-arg1 initial-arg2  initial-arg3"))
	})
}
