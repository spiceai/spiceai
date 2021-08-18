package openai_gym

import (
	"time"
)

const (
	OpenAIGymConnectorName string = "openai-gym"
)

type OpenAIGymConnector struct {
	environment string
}

func NewOpenAIGymConnector() *OpenAIGymConnector {
	return &OpenAIGymConnector{}
}

func (c *OpenAIGymConnector) Init(params map[string]string) error {
	c.environment = params["environment"]
	return nil
}

func (c *OpenAIGymConnector) FetchData(epoch time.Time, period time.Duration, interval time.Duration) ([]byte, error) {
	return nil, nil
}
