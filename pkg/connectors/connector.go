package connectors

import (
	"fmt"
	"time"

	"github.com/spiceai/spice/pkg/observations"
)

const (
	CsvConnectorId       = "csv"
	InfluxDbConnectorId  = "influxdb"
	OpenAIGymConnectorId = "openai-gym"
	StatefulConnectorId  = "stateful"
)

type Connector interface {
	Type() string
	Initialize() error
	FetchData(epoch time.Time, period time.Duration, interval time.Duration) ([]observations.Observation, error)
}

func NewConnector(id string, params map[string]string) (Connector, error) {
	switch id {
	case CsvConnectorId:
		return NewCsvConnector(params), nil
	case InfluxDbConnectorId:
		return NewInfluxDbConnector(params), nil
	case OpenAIGymConnectorId:
		return NewOpenAIGymConnector(params), nil
	case StatefulConnectorId:
		return NewStateConnector(params), nil
	}

	return nil, fmt.Errorf("unknown connector '%s'", id)
}
