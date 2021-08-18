package openai_gym

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCsv(t *testing.T) {
	t.Run("Init()", testInitFunc())
}

// Tests "Init()"
func testInitFunc() func(*testing.T) {
	p := NewOpenAIGymProcessor()

	params := map[string]string{}

	return func(t *testing.T) {
		err := p.Init(params)
		assert.NoError(t, err)
	}
}
