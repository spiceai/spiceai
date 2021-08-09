package connectors

import (
	"sync"
	"time"

	"github.com/spiceai/spice/pkg/observations"
)

type StateConnector struct {
	params     map[string]string
	state      []observations.Observation
	stateMutex sync.RWMutex
}

func NewStateConnector(params map[string]string) Connector {
	return &StateConnector{
		params:     params,
		stateMutex: sync.RWMutex{},
	}
}

func (c *StateConnector) Initialize() error {
	c.stateMutex.Lock()
	defer c.stateMutex.Unlock()

	c.state = make([]observations.Observation, 0)

	return nil
}

func (c *StateConnector) AppendData(observations []observations.Observation) {
	c.stateMutex.Lock()
	defer c.stateMutex.Unlock()

	c.state = append(c.state, observations...)
}

func (c *StateConnector) FetchData(period time.Duration, interval time.Duration) ([]observations.Observation, error) {
	return c.state, nil
}
