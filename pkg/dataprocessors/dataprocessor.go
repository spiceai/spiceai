package dataprocessors

import (
	"fmt"

	"github.com/spiceai/spiceai/pkg/dataprocessors/csv"
	"github.com/spiceai/spiceai/pkg/dataprocessors/flux"
	"github.com/spiceai/spiceai/pkg/dataprocessors/openai_gym"
	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/spiceai/spiceai/pkg/state"
)

type DataProcessor interface {
	Init(params map[string]string) error
	OnData(data []byte) ([]byte, error)
	GetObservations() ([]observations.Observation, error)
	GetState(fields *[]string) ([]*state.State, error)
}

func NewDataProcessor(name string) (DataProcessor, error) {
	switch name {
	case csv.CsvProcessorName:
		return csv.NewCsvProcessor(), nil
	case flux.FluxCsvProcessorName:
		return flux.NewFluxCsvProcessor(), nil
	case openai_gym.OpenAIGymProcessorName:
		return openai_gym.NewOpenAIGymProcessor(), nil
	}

	return nil, fmt.Errorf("unknown processor '%s'", name)
}
