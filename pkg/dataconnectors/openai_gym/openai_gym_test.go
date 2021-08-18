package openai_gym_test

import (
	"testing"
	"time"

	"github.com/spiceai/spice/pkg/dataconnectors/openai_gym"
	"github.com/stretchr/testify/assert"
)

func TestOpenAIGymConnector(t *testing.T) {
	params := make(map[string]string)

	t.Run("OpenAI Gym Initialize()", testOpenAIGymInitializeFunc(params))
	t.Run("OpenAI Gym FetchData()", testOpenAIGymFetchDataFunc(params))
}

func testOpenAIGymInitializeFunc(params map[string]string) func(*testing.T) {
	c := openai_gym.NewOpenAIGymConnector()
	return func(t *testing.T) {
		err := c.Init(params)
		assert.NoError(t, err)
	}
}

func testOpenAIGymFetchDataFunc(params map[string]string) func(*testing.T) {
	c := openai_gym.NewOpenAIGymConnector()
	return func(t *testing.T) {
		err := c.Init(params)
		assert.NoError(t, err)

		data, err := c.FetchData(time.Time{}, time.Hour*24*365*10, time.Minute*1)
		if assert.NoError(t, err) {
			assert.EqualValues(t, []byte(nil), data)
		}
	}
}
