package observations_test

import (
	"io/ioutil"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/data-components-contrib/dataprocessors"
	"github.com/spiceai/data-components-contrib/dataprocessors/csv"
	"github.com/spiceai/spiceai/pkg/observations"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/observations"))

func TestObservations(t *testing.T) {
	t.Run("GetCsv() - All headers", testGetCsvAllHeadersFunc())
	t.Run("GetCsv() - Select headers", testGetCsvSelectHeadersFunc())
}

// Tests "GetCsv() - All headers
func testGetCsvAllHeadersFunc() func(*testing.T) {
	return func(t *testing.T) {
		data, err := ioutil.ReadFile("../../test/assets/data/csv/COINBASE_BTCUSD, 30.csv")
		if err != nil {
			t.Fatal(err)
		}

		dp, err := dataprocessors.NewDataProcessor(csv.CsvProcessorName)
		if err != nil {
			t.Error(err)
		}

		err = dp.Init(nil)
		if err != nil {
			t.Fatal(err)
		}

		_, err = dp.OnData(data)
		if err != nil {
			t.Fatal(err)
		}

		newObservations, err := dp.GetObservations()
		if err != nil {
			t.Error(err)
			return
		}

		headerLine := "open,high,low,close,volume"
		headers := strings.Split(headerLine, ",")

		csv := observations.GetCsv(headers, newObservations)

		snapshotter.SnapshotT(t, csv)
	}
}

// Tests "GetCsv() - Select headers
func testGetCsvSelectHeadersFunc() func(*testing.T) {
	return func(t *testing.T) {
		data, err := ioutil.ReadFile("../../test/assets/data/csv/COINBASE_BTCUSD, 30.csv")
		if err != nil {
			t.Fatal(err)
		}

		dp, err := dataprocessors.NewDataProcessor(csv.CsvProcessorName)
		if err != nil {
			t.Error(err)
		}

		err = dp.Init(nil)
		if err != nil {
			t.Fatal(err)
		}

		_, err = dp.OnData(data)
		if err != nil {
			t.Fatal(err)
		}

		newObservations, err := dp.GetObservations()
		if err != nil {
			t.Error(err)
			return
		}

		headers := strings.Split("open,high,low,close,volume", ",")
		csv := observations.GetCsv(headers, newObservations)

		snapshotter.SnapshotT(t, csv)
	}
}
