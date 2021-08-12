package connectors_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/spiceai/spice/pkg/connectors"
	"github.com/spiceai/spice/pkg/observations"
	"github.com/stretchr/testify/assert"
)

func TestConnectors(t *testing.T) {
	// OpenAI Gym connector
	params := make(map[string]string)
	openAIGymConnector, err := connectors.NewConnector("openai-gym", params)
	if assert.NoError(t, err) {
		t.Run("OpenAI Gym Initialize()", testOpenAIGymInitializeFunc(openAIGymConnector.(*connectors.OpenAIGymConnector)))
		t.Run("OpenAI Gym FetchData()", testOpenAIGymFetchDataFunc(openAIGymConnector.(*connectors.OpenAIGymConnector)))
	}

	// State connector
	params = make(map[string]string)
	stateConnector, err := connectors.NewConnector("stateful", params)
	if assert.NoError(t, err) {
		t.Run("State Initialize()", testStateInitializeFunc(stateConnector.(*connectors.StateConnector)))
		t.Run("State FetchData()", testStateFetchDataFunc(stateConnector.(*connectors.StateConnector)))
	}

	// Csv connector
	csvFilesToTest := []string{"COINBASE_BTCUSD, 30.csv"}
	for _, csvFileToTest := range csvFilesToTest {
		csvPath := filepath.Join("../../test/assets/data/csv", csvFileToTest)

		params := make(map[string]string)
		params["path"] = csvPath

		c := connectors.NewCsvConnector(params)

		t.Run(fmt.Sprintf("Initialize() - %s", csvFileToTest), testCsvInitializeFunc(c))
		t.Run(fmt.Sprintf("FetchData() - %s", csvFileToTest), testCsvFetchDataFunc(c))
	}
}

func testOpenAIGymInitializeFunc(c *connectors.OpenAIGymConnector) func(*testing.T) {
	return func(t *testing.T) {
		err := c.Initialize()
		assert.NoError(t, err)
	}
}

func testOpenAIGymFetchDataFunc(c *connectors.OpenAIGymConnector) func(*testing.T) {
	return func(t *testing.T) {
		data, err := c.FetchData(time.Time{}, time.Hour*24*365*10, time.Minute*1)
		if assert.NoError(t, err) {
			assert.EqualValues(t, []observations.Observation{}, data)
		}
	}
}

func testStateInitializeFunc(c *connectors.StateConnector) func(*testing.T) {
	return func(t *testing.T) {
		err := c.Initialize()
		assert.NoError(t, err)
	}
}

func testStateFetchDataFunc(c *connectors.StateConnector) func(*testing.T) {
	return func(t *testing.T) {
		data, err := c.FetchData(time.Time{}, time.Hour*24*365*10, time.Minute*1)
		if assert.NoError(t, err) {
			assert.EqualValues(t, []observations.Observation{}, data)
		}
	}
}

func testCsvInitializeFunc(c *connectors.CsvConnector) func(*testing.T) {
	return func(t *testing.T) {
		err := c.Initialize()
		if err != nil {
			t.Error(err)
		}
	}
}

func testCsvFetchDataFunc(c *connectors.CsvConnector) func(*testing.T) {
	return func(t *testing.T) {
		data, err := c.FetchData(time.Unix(1605312000, 0), time.Hour*24*365*10, time.Minute*1)
		if err != nil {
			t.Error(err)
		}

		expectedCount := 1443
		actualCount := len(data)
		if expectedCount != actualCount {
			t.Errorf("Expected %d records, got %d", expectedCount, actualCount)
			return
		}
	}
}
