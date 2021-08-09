package connectors

import (
	"fmt"
	"time"

	"github.com/spiceai/spice/pkg/observations"
)

type Connector interface {
	Initialize() error
	FetchData(period time.Duration, interval time.Duration) ([]observations.Observation, error)
}

func NewConnector(id string, params map[string]string) (Connector, error) {
	switch id {
	case "csv":
		return NewCsvConnector(params), nil
	case "influxdb":
		return NewInfluxDbConnector(params), nil
	case "openai-gym":
		return NewOpenAIGymConnector(params), nil
	case "stateful":
		return NewStateConnector(params), nil
	}

	return nil, fmt.Errorf("unknown connector '%s'", id)
}
