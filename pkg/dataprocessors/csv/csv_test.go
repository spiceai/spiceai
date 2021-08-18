package csv

import (
	"sort"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/spice/pkg/dataconnectors/file"
	"github.com/spiceai/spice/pkg/observations"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../../test/assets/snapshots/dataprocessors/csv"))

func TestCsv(t *testing.T) {
	localFileConnector := file.NewFileConnector()
	err := localFileConnector.Init(map[string]string{
		"path":  "../../../test/assets/data/csv/COINBASE_BTCUSD, 30.csv",
		"watch": "false",
	})
	if err != nil {
		t.Fatal(err.Error())
	}
	localData, err := localFileConnector.FetchData(time.Unix(1605312000, 0), 7*24*time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err.Error())
	}

	globalFileConnector := file.NewFileConnector()
	err = globalFileConnector.Init(map[string]string{
		"path":  "../../../test/assets/data/csv/trader_input.csv",
		"watch": "false",
	})
	if err != nil {
		t.Fatal(err.Error())
	}
	globalData, err := globalFileConnector.FetchData(time.Unix(1605312000, 0), 7*24*time.Hour, time.Hour)
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Run("Init()", testInitFunc())
	t.Run("GetObservations()", testGetObservationsFunc(localData))
	t.Run("GetState()", testGetStateFunc(globalData))
	t.Run("getColumnMappings()", testgetColumnMappingsFunc())
}

func BenchmarkGetObservations(b *testing.B) {
	localFileConnector := file.NewFileConnector()
	err := localFileConnector.Init(map[string]string{
		"path":  "../../test/assets/data/csv/COINBASE_BTCUSD, 30.csv",
		"watch": "false",
	})
	if err != nil {
		b.Error(err)
	}

	b.Run("GetObservations()", benchGetObservationsFunc(localFileConnector))
}

// Tests "Init()"
func testInitFunc() func(*testing.T) {
	p := NewCsvProcessor()

	params := map[string]string{}

	return func(t *testing.T) {
		err := p.Init(params)
		assert.NoError(t, err)
	}
}

// Tests "GetObservations()"
func testGetObservationsFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		dp := NewCsvProcessor()
		err := dp.Init(nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualObservations, err := dp.GetObservations()
		if err != nil {
			t.Error(err)
			return
		}

		expectedFirstObservation := observations.Observation{
			Time: 1605312000,
			Data: map[string]float64{
				"open":   16339.56,
				"high":   16339.6,
				"low":    16240,
				"close":  16254.51,
				"volume": 274.42607,
			},
		}
		assert.Equal(t, expectedFirstObservation, actualObservations[0], "First Observation not correct")

		snapshotter.SnapshotT(t, actualObservations)
	}
}

// Tests "GetState()"
func testGetStateFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		dp := NewCsvProcessor()
		err := dp.Init(nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualState, err := dp.GetState(nil)
		if err != nil {
			t.Error(err)
			return
		}

		assert.Equal(t, 2, len(actualState), "expected two state objects")

		sort.Slice(actualState, func(i, j int) bool {
			return actualState[i].Path() < actualState[j].Path()
		})

		assert.Equal(t, "coinbase.btcusd", actualState[0].Path(), "expected path incorrect")
		assert.Equal(t, "local.portfolio", actualState[1].Path(), "expected path incorrect")

		expectedFirstObservation := observations.Observation{
			Time: 1626697480,
			Data: map[string]float64{
				"price": 31232.709090909084,
			},
		}

		actualObservations := actualState[0].Observations()
		assert.Equal(t, expectedFirstObservation, actualState[0].Observations()[0], "First Observation not correct")
		assert.Equal(t, 57, len(actualObservations), "number of observations incorrect")

		expectedObservations := make([]observations.Observation, 0)
		assert.Equal(t, expectedObservations, actualState[1].Observations(), "Observations not correct")
	}
}

// Tests "getColumnMappings()"
func testgetColumnMappingsFunc() func(*testing.T) {
	return func(t *testing.T) {
		headers := []string{"time", "local.portfolio.usd_balance", "local.portfolio.btc_balance", "coinbase.btcusd.price"}

		colToPath, colToFieldName, err := getColumnMappings(headers)
		if err != nil {
			t.Error(err)
			return
		}

		expectedColToPath := []string{"local.portfolio", "local.portfolio", "coinbase.btcusd"}
		assert.Equal(t, expectedColToPath, colToPath, "column to path mapping incorrect")

		expectedColToFieldName := []string{"usd_balance", "btc_balance", "price"}
		assert.Equal(t, expectedColToFieldName, colToFieldName, "column to path mapping incorrect")
	}
}

// Benchmark "GetObservations()"
func benchGetObservationsFunc(c *file.FileConnector) func(*testing.B) {
	return func(b *testing.B) {
		dp := NewCsvProcessor()
		err := dp.Init(nil)
		if err != nil {
			b.Error(err)
		}

		for i := 0; i < 10; i++ {
			_, err := dp.GetObservations()
			if err != nil {
				b.Fatal(err.Error())
			}
		}
	}
}
