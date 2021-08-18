package openai_gym

import (
	"github.com/spiceai/spice/pkg/observations"
	"github.com/spiceai/spice/pkg/state"
)

const (
	OpenAIGymProcessorName string = "openai-gym"
)

type OpenAIGymProcessor struct{}

func NewOpenAIGymProcessor() *OpenAIGymProcessor {
	return &OpenAIGymProcessor{}
}

func (p *OpenAIGymProcessor) Init(params map[string]string) error {
	return nil
}

func (p *OpenAIGymProcessor) OnData(data []byte) ([]byte, error) {
	return data, nil
}

func (p *OpenAIGymProcessor) GetObservations() ([]observations.Observation, error) {
	return nil, nil
}

func (p *OpenAIGymProcessor) GetState(validFields *[]string) ([]*state.State, error) {
	return nil, nil
}
