package csv

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/spice/pkg/observations"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/csv"))

func TestCsv(t *testing.T) {
	localCsvFilePath := "../../test/assets/data/csv/COINBASE_BTCUSD, 30.csv"
	localCsvData, err := ioutil.ReadFile(localCsvFilePath)
	if err != nil {
		t.Error(err)
		return
	}

	globalCsvFilePath := "../../test/assets/data/csv/trader_input.csv"
	globalCsvData, err := ioutil.ReadFile(globalCsvFilePath)
	if err != nil {
		t.Error(err)
		return
	}

	t.Run("ProcessCsv()", testProcessCsvFunc(localCsvData))
	t.Run("ProcessCsvByPath()", testProcessCsvByPathFunc(globalCsvData))
	t.Run("getColumnMappings()", testgetColumnMappingsFunc())
}

func BenchmarkProcessCsv(b *testing.B) {
	csvFilePath := "../../test/assets/data/csv/COINBASE_BTCUSD, 30.csv"
	csvData, err := ioutil.ReadFile(csvFilePath)
	if err != nil {
		b.Error(err)
		return
	}

	b.Run("ProcessCsv()", benchProcessCsvFunc(csvData))
}

// Tests "ProcessCsv()"
func testProcessCsvFunc(csvData []byte) func(*testing.T) {
	return func(t *testing.T) {
		reader := bytes.NewReader(csvData)

		actualObservations, err := ProcessCsv(reader)
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

// Tests "ProcessCsvByPath()"
func testProcessCsvByPathFunc(csvData []byte) func(*testing.T) {
	return func(t *testing.T) {
		reader := bytes.NewReader(csvData)

		actualState, err := ProcessCsvByPath(reader, nil)
		if err != nil {
			t.Error(err)
			return
		}

		fmt.Printf("%v\n\n", actualState)

		assert.Equal(t, 2, len(actualState), "expected two state objects")
		assert.Equal(t, "local.portfolio", actualState[0].Path(), "expected path incorrect")
		assert.Equal(t, "coinbase.btcusd", actualState[1].Path(), "expected path incorrect")

		expectedObservations := make([]observations.Observation, 0)
		assert.Equal(t, expectedObservations, actualState[0].Observations(), "Observations not correct")

		expectedFirstObservation := observations.Observation{
			Time: 1626697480,
			Data: map[string]float64{
				"price": 31232.709090909084,
			},
		}

		actualObservations := actualState[1].Observations()
		assert.Equal(t, expectedFirstObservation, actualState[1].Observations()[0], "First Observation not correct")
		assert.Equal(t, 57, len(actualObservations), "number of observations incorrect")
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

// Benchmark "GetCsv()"
func benchProcessCsvFunc(csvData []byte) func(*testing.B) {
	return func(b *testing.B) {
		for i := 0; i < 10; i++ {
			reader := bytes.NewReader(csvData)
			ProcessCsv(reader)
		}
	}
}
