package connectors

import (
	"time"

	"github.com/spiceai/spice/pkg/observations"
)

type OpenAIGymConnector struct {
	state []observations.Observation
}

func NewOpenAIGymConnector(params map[string]string) Connector {
	return &StateConnector{}
}

func (c *OpenAIGymConnector) Initialize() error {
	c.state = make([]observations.Observation, 0)

	return nil
}

func (c *OpenAIGymConnector) FetchData(period time.Duration, interval time.Duration) ([]observations.Observation, error) {
	return c.state, nil
}
