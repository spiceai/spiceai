package observations_test

import (
	"os"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/data-components-contrib/dataprocessors"
	"github.com/spiceai/data-components-contrib/dataprocessors/csv"
	"github.com/spiceai/spiceai/pkg/observations"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/observations"))

func TestObservationsCategorical(t *testing.T) {
	observation := &observations.Observation{
		Measurements: map[string]float64{"foo": 1.0},
		Categories:   map[string]string{"color": "red"},
	}

	csv := observations.GetCsv([]string{"foo", "color"}, []string{}, []observations.Observation{*observation})

	snapshotter.SnapshotT(t, csv)
}

func TestObservations(t *testing.T) {
	t.Run("GetCsv() - All headers", testGetCsvAllHeadersFunc())
	t.Run("GetCsv() - Select headers", testGetCsvSelectHeadersFunc())
	t.Run("GetCsv() - With tags", testGetCsvWithTagsFunc())
}

// Tests "GetCsv() - With tags
func testGetCsvWithTagsFunc() func(*testing.T) {
	return func(t *testing.T) {
		data, err := os.ReadFile("../../test/assets/data/csv/csv_data_with_tags.csv")
		if err != nil {
			t.Fatal(err)
		}

		dp, err := dataprocessors.NewDataProcessor(csv.CsvProcessorName)
		if err != nil {
			t.Error(err)
		}

		measurements := map[string]string{
			"eventId": "event_id",
			"height":  "h",
			"rating":  "rating",
			"speed":   "speed",
			"target":  "target",
		}

		err = dp.Init(nil, measurements, nil, nil)
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

		headerLine := "eventId,height,rating,speed,target,_tags"
		headers := strings.Split(headerLine, ",")
		tags := []string{"tagA", "tagB"}

		csv := observations.GetCsv(headers, tags, newObservations)

		snapshotter.SnapshotT(t, csv)
	}
}

// Tests "GetCsv() - All headers
func testGetCsvAllHeadersFunc() func(*testing.T) {
	return func(t *testing.T) {
		data, err := os.ReadFile("../../test/assets/data/csv/COINBASE_BTCUSD, 30.csv")
		if err != nil {
			t.Fatal(err)
		}

		dp, err := dataprocessors.NewDataProcessor(csv.CsvProcessorName)
		if err != nil {
			t.Error(err)
		}

		measurements := map[string]string{
			"open":   "open",
			"high":   "high",
			"low":    "low",
			"close":  "close",
			"volume": "volume",
		}

		err = dp.Init(nil, measurements, nil, nil)
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

		csv := observations.GetCsv(headers, nil, newObservations)

		snapshotter.SnapshotT(t, csv)
	}
}

// Tests "GetCsv() - Select headers
func testGetCsvSelectHeadersFunc() func(*testing.T) {
	return func(t *testing.T) {
		data, err := os.ReadFile("../../test/assets/data/csv/COINBASE_BTCUSD, 30.csv")
		if err != nil {
			t.Fatal(err)
		}

		dp, err := dataprocessors.NewDataProcessor(csv.CsvProcessorName)
		if err != nil {
			t.Error(err)
		}

		measurements := map[string]string{
			"open":   "open",
			"high":   "high",
			"low":    "low",
			"close":  "close",
			"volume": "volume",
		}

		err = dp.Init(nil, measurements, nil, nil)
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
		csv := observations.GetCsv(headers, nil, newObservations)

		snapshotter.SnapshotT(t, csv)
	}
}
