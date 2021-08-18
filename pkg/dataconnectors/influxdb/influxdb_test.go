package influxdb_test

import (
	"testing"

	"github.com/spiceai/spice/pkg/dataconnectors/influxdb"
	"github.com/stretchr/testify/assert"
)

func TestInfluxDbConnector(t *testing.T) {
	params := map[string]string{
		"url":   "fake-url-for-test",
		"token": "fake-token-for-test",
	}

	t.Run("Init()", testInitFunc(params))
}

func testInitFunc(params map[string]string) func(*testing.T) {
	c := influxdb.NewInfluxDbConnector()

	return func(t *testing.T) {
		err := c.Init(params)
		assert.NoError(t, err)
	}
}
