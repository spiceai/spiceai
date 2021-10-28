package aiengine

import (
	"os"
	"strings"
	"testing"
	"time"

	"github.com/spiceai/data-components-contrib/dataprocessors"
	"github.com/spiceai/data-components-contrib/dataprocessors/csv"
	"github.com/spiceai/data-components-contrib/dataprocessors/json"
	"github.com/spiceai/spiceai/pkg/dataspace"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/state"
	"github.com/stretchr/testify/assert"
)

func TestObservations(t *testing.T) {
	t.Run("getData() - All headers with preview", testGetCsvAllHeadersWithPreviewFunc())
	t.Run("getData() - Select headers with preview", testGetCsvSelectHeadersWithPreviewFunc())
	t.Run("getData() - With tags", testGetDataWithTagsFunc())
	t.Run("getData() - With categories", testGetDataWithCategoriesFunc())
	t.Run("getAddDataRequest()", testGetAddDataRequestFunc())
}

func testGetAddDataRequestFunc() func(*testing.T) {
	return func(t *testing.T) {
		data, err := os.ReadFile("../../test/assets/data/json/event_stream_categories.json")
		if err != nil {
			t.Fatal(err)
		}

		pod, err := pods.LoadPodFromManifest("../../test/assets/pods/manifests/event-categories.yaml")
		if err != nil {
			t.Error(err)
			return
		}

		dp, err := dataprocessors.NewDataProcessor(json.JsonProcessorName)
		if err != nil {
			t.Error(err)
		}

		measurements := map[string]string{
			"duration":     "length_of_time",
			"guest_count":  "num_guests",
			"ticket_price": "ticket_price",
		}

		categories := map[string]string{
			"event_type":      "event_type",
			"target_audience": "target_audience",
		}

		tagSelectors := []string {
			"tags",
		}

		err = dp.Init(nil, measurements, categories, tagSelectors)
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

		measurementNames := []string{"duration", "guest_count", "ticket_price"}
		categoryNames := []string{"event_type", "target_audience"}
		tags := []string{"tagA", "tagB", "tagC"}

		s := state.NewState("event.stream", measurementNames, categoryNames, tags, newObservations)

		addDataRequest := getAddDataRequest(pod, s)

		assert.Equal(t, "event-categories", addDataRequest.Pod)

		csvData := strings.TrimSpace(addDataRequest.CsvData)

		csvLines := strings.Split(csvData, "\n")
		if assert.Len(t, csvLines, len(newObservations)+1, "number of csv lines does not match observations") {
			for _, csvLine := range csvLines {
				csvFields := strings.Split(csvLine, ",")
				if !assert.Len(t, csvFields, 15, "number of fields does not match state") {
					break
				}
			}
		}

		snapshotter.SnapshotT(t, csvData)
	}
}

// Tests "GetCsv() - All headers with preview
func testGetCsvAllHeadersWithPreviewFunc() func(*testing.T) {
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

		csv := strings.Builder{}
		epoch := time.Unix(1605312001, 0)
		headerLine := "open,high,low,close,volume"
		measurementNames := strings.Split(headerLine, ",")
		tags := make([]string, 0)

		actualPreviewCsv := getData(&csv, epoch, measurementNames, nil, tags, newObservations, 5)

		expectedPreviewCsv := `1605313800,16256.42,16305,16248.6,16305,110.91971
1605315600,16303.88,16303.88,16210.99,16222.16,231.64805
1605317400,16221.78,16246.92,16200.01,16214.15,135.86161
1605319200,16214.26,16234,16175.62,16223.08,164.32561
`

		assert.Equal(t, expectedPreviewCsv, actualPreviewCsv, "preview csv did not match")

		snapshotter.SnapshotT(t, csv.String())
	}
}

// Tests "GetCsv() - Select headers with preview
func testGetCsvSelectHeadersWithPreviewFunc() func(*testing.T) {
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

		csv := strings.Builder{}
		epoch := time.Unix(1605312000, 0)
		measurementNames := strings.Split("open,high,low,close,volume", ",")
		tags := make([]string, 0)

		actualPreviewCsv := getData(&csv, epoch, measurementNames, nil, tags, newObservations, 5)

		expectedPreviewCsv := `1605312000,16339.56,16339.6,16240,16254.51,274.42607
1605313800,16256.42,16305,16248.6,16305,110.91971
1605315600,16303.88,16303.88,16210.99,16222.16,231.64805
1605317400,16221.78,16246.92,16200.01,16214.15,135.86161
1605319200,16214.26,16234,16175.62,16223.08,164.32561
`

		assert.Equal(t, expectedPreviewCsv, actualPreviewCsv, "preview csv did not match")
	}
}

func testGetDataWithTagsFunc() func(*testing.T) {
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

		tagSelectors := []string{
			"tags1",
			"tags2",
			"tags3",
			"_tags",
		}

		err = dp.Init(nil, measurements, nil, tagSelectors)
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

		csv := strings.Builder{}
		epoch := time.Unix(1610057400, 0)
		measurementNames := []string{"eventId", "height", "rating", "speed", "target"}
		tags := []string{"tagA", "tagB", "tagC", "tagD"}

		actualPreviewCsv := getData(&csv, epoch, measurementNames, nil, tags, newObservations, 5)

		snapshotter.SnapshotT(t, actualPreviewCsv)
	}
}

func testGetDataWithCategoriesFunc() func(*testing.T) {
	return func(t *testing.T) {
		data, err := os.ReadFile("../../test/assets/data/json/event_stream_categories.json")
		if err != nil {
			t.Fatal(err)
		}

		dp, err := dataprocessors.NewDataProcessor(json.JsonProcessorName)
		if err != nil {
			t.Error(err)
		}

		measurements := map[string]string{
			"duration":     "length_of_time",
			"guest_count":  "num_guests",
			"ticket_price": "ticket_price",
		}

		categories := map[string]string{
			"event_type":      "event_type",
			"target_audience": "target_audience",
		}

		err = dp.Init(nil, measurements, categories, nil)
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

		categoriesList := []*dataspace.CategoryInfo{
			{Name: "event_type", Values: []string{"dinner", "party", "dance", "concert", "football_game"}},
			{Name: "target_audience", Values: []string{"employees", "investors", "cohort_a"}},
		}

		csv := strings.Builder{}
		epoch := time.Unix(1610057400, 0)
		measurementNames := []string{"duration", "guest_count", "ticket_price"}
		tags := []string{"tagA", "tagB", "tagC"}

		t.Log(measurementNames)
		getData(&csv, epoch, measurementNames, categoriesList, tags, newObservations, 5)

		snapshotter.SnapshotT(t, csv.String())
	}
}
