package dataconnectors

import (
	"fmt"
	"time"

	"github.com/spiceai/spiceai/pkg/dataconnectors/file"
	"github.com/spiceai/spiceai/pkg/dataconnectors/influxdb"
	"github.com/spiceai/spiceai/pkg/dataconnectors/openai_gym"
)

type DataConnector interface {
	Init(params map[string]string) error
	FetchData(epoch time.Time, period time.Duration, interval time.Duration) ([]byte, error)
}

func NewDataConnector(name string) (DataConnector, error) {
	switch name {
	case file.FileConnectorName:
		return file.NewFileConnector(), nil
	case influxdb.InfluxDbConnectorName:
		return influxdb.NewInfluxDbConnector(), nil
	case openai_gym.OpenAIGymConnectorName:
		return openai_gym.NewOpenAIGymConnector(), nil
	}

	return nil, fmt.Errorf("unknown data connector '%s'", name)
}
