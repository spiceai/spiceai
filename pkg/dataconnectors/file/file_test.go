package file_test

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/spiceai/pkg/dataconnectors/file"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../../test/assets/snapshots/dataconnectors/file"))

func TestFileConnector(t *testing.T) {
	filesToTest := []string{"COINBASE_BTCUSD, 30.csv"}
	for _, fileToTest := range filesToTest {
		filePath := filepath.Join("../../../test/assets/data/csv", fileToTest)

		params := make(map[string]string)
		params["path"] = filePath

		t.Run(fmt.Sprintf("Init() - %s", fileToTest), testInitFunc(params))
		t.Run(fmt.Sprintf("FetchData() - %s", fileToTest), testFetchDataFunc(params))
		t.Run(fmt.Sprintf("FetchData() twice - %s", fileToTest), testFetchDataTwiceFunc(params))
	}
}

func testInitFunc(params map[string]string) func(*testing.T) {
	c := file.NewFileConnector()

	return func(t *testing.T) {
		err := c.Init(params)
		assert.NoError(t, err)
	}
}

func testFetchDataFunc(params map[string]string) func(*testing.T) {
	c := file.NewFileConnector()

	return func(t *testing.T) {
		err := c.Init(params)
		assert.NoError(t, err)

		data, err := c.FetchData(time.Unix(1605312000, 0), time.Hour*24*365*10, time.Minute*1)
		assert.NoError(t, err)

		snapshotter.SnapshotT(t, string(data))
	}
}

func testFetchDataTwiceFunc(params map[string]string) func(*testing.T) {
	c := file.NewFileConnector()

	return func(t *testing.T) {
		err := c.Init(params)
		assert.NoError(t, err)

		data, err := c.FetchData(time.Unix(1605312000, 0), time.Hour*24*365*10, time.Minute*1)
		assert.NoError(t, err)

		snapshotter.SnapshotT(t, string(data))

		data2, err := c.FetchData(time.Unix(1605312000, 0), time.Hour*24*365*10, time.Minute*1)
		assert.NoError(t, err)

		snapshotter.SnapshotT(t, string(data2))
	}
}
