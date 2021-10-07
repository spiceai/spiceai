package metal

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMetaContext(t *testing.T) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Name()", func(t *testing.T) {
		c := NewMetalContext()
		assert.Equal(t, "metal", c.Name())
	})

	t.Run("GetRunCmd() - production mode", func(t *testing.T) {
		c := NewMetalContext()

		cmd, err := c.GetRunCmd("test-manifest-path")
		assert.NoError(t, err)

		if assert.Len(t, cmd.Args, 2) {
			assert.Equal(t, "/.spice/bin/spiced", strings.TrimPrefix(cmd.Args[0], homeDir))
			assert.Equal(t, "test-manifest-path", cmd.Args[1])
		}
	})

	t.Run("GetRunCmd() - development mode", func(t *testing.T) {
		c := NewMetalContext()
		if err := c.Init(true); err != nil {
			t.Fatal(err)
		}

		cmd, err := c.GetRunCmd("test-manifest-path")
		assert.NoError(t, err)

		if assert.Len(t, cmd.Args, 3) {
			assert.Equal(t, "/.spice/bin/spiced", strings.TrimPrefix(cmd.Args[0], homeDir))
			assert.Equal(t, "test-manifest-path", cmd.Args[1])
			assert.Equal(t, "--development", cmd.Args[2])
		}
	})
}
