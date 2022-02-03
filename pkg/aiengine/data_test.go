package aiengine

import (
	"bytes"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v7/arrow/csv"
	"github.com/spiceai/data-components-contrib/dataprocessors"
	// "github.com/spiceai/data-components-contrib/dataprocessors/csv"
	"github.com/spiceai/data-components-contrib/dataprocessors/json"
	// "github.com/spiceai/spiceai/pkg/dataspace"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/state"

	// spice_time "github.com/spiceai/spiceai/pkg/time"
	"github.com/stretchr/testify/assert"
)

// func TestGetData(t *testing.T) {
// 	t.Run("getData() - All headers with preview", testGetCsvAllHeadersWithPreviewFunc())
// 	t.Run("getData() - Select headers with preview", testGetCsvSelectHeadersWithPreviewFunc())
// 	t.Run("getData() - With tags", testGetDataWithTagsFunc())
// 	t.Run("getData() - With categories", testGetDataWithCategoriesFunc())
// }

func TestGetAddDataRequest(t *testing.T) {
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

	tagSelectors := []string{
		"tags",
	}

	err = dp.Init(nil, nil, measurements, categories, tagSelectors)
	if err != nil {
		t.Fatal(err)
	}

	_, err = dp.OnData(data)
	if err != nil {
		t.Fatal(err)
	}

	record, err := dp.GetRecord()
	if err != nil {
		t.Error(err)
		return
	}

	newState := state.NewState("event.stream", record)
	pod.AddLocalState(newState)

	addDataRequest := getAddDataRequest(pod, newState, "")

	assert.Equal(t, "event-categories", addDataRequest.Pod)

	record = getProcessedRecord(pod, newState)
	bytebuffer := new(bytes.Buffer)
	writer := csv.NewWriter(bytebuffer, record.Schema(), csv.WithHeader(true), csv.WithComma(','), csv.WithNullWriter(""))
	err = writer.Write(record)
	if err != nil {
		t.Error(err)
		return
	}
	csvData := bytebuffer.String()

	expectedNumberOfFields := 1 /* time */ + len(measurements) + 5 /* category event_type */ + 3 /* category target_audience */ + 3 /* tags */
	for _, fields := range pod.TimeCategories() {
		expectedNumberOfFields += len(fields)
	}

	csvLines := strings.Split(csvData, "\n")
	// Arrow CSV writter add an empty line at the end so the number of lines should be header + rows + 1
	if assert.Equal(t, len(csvLines), int(record.NumRows())+2, "number of csv lines does not match record") {
		for _, csvLine := range csvLines[:len(csvLines)-1] {
			csvFields := strings.Split(csvLine, ",")
			if !assert.Equal(t, expectedNumberOfFields, len(csvFields), "number of fields does not match state") {
				break
			}
		}
	}

	snapshotter.SnapshotT(t, csvData)
}

// Tests "GetCsv() - All headers with preview
// func testGetCsvAllHeadersWithPreviewFunc() func(*testing.T) {
// 	return func(t *testing.T) {
// 		data, err := os.ReadFile("../../test/assets/data/csv/COINBASE_BTCUSD, 30.csv")
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		dp, err := dataprocessors.NewDataProcessor(csv.CsvProcessorName)
// 		if err != nil {
// 			t.Error(err)
// 		}

// 		measurements := map[string]string{
// 			"open":   "open",
// 			"high":   "high",
// 			"low":    "low",
// 			"close":  "close",
// 			"volume": "volume",
// 		}

// 		err = dp.Init(nil, nil, measurements, nil, nil)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		_, err = dp.OnData(data)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		record, err := dp.GetRecord()
// 		if err != nil {
// 			t.Error(err)
// 			return
// 		}

// 		newObservations, err := dataspace.ArrowToObservations(record)
// 		record.Release()
// 		if err != nil {
// 			t.Error(err)
// 			return
// 		}

// 		csv := strings.Builder{}
// 		epoch := time.Unix(1605312001, 0)
// 		headerLine := "open,high,low,close,volume"
// 		timeCategoryNames := []string{}
// 		timeCategoryFields := spice_time.GenerateTimeCategoryFields(timeCategoryNames...)
// 		measurementNames := strings.Split(headerLine, ",")
// 		tags := make([]string, 0)

// 		actualPreviewCsv := getData(&csv, epoch, timeCategoryNames, timeCategoryFields, measurementNames, nil, tags, newObservations, 5)

// 		expectedPreviewCsv := `1605313800,16256.42,16305,16248.6,16305,110.91971
// 1605315600,16303.88,16303.88,16210.99,16222.16,231.64805
// 1605317400,16221.78,16246.92,16200.01,16214.15,135.86161
// 1605319200,16214.26,16234,16175.62,16223.08,164.32561
// `

// 		assert.Equal(t, expectedPreviewCsv, actualPreviewCsv, "preview csv did not match")

// 		snapshotter.SnapshotT(t, csv.String())
// 	}
// }

// Tests "GetCsv() - Select headers with preview
// func testGetCsvSelectHeadersWithPreviewFunc() func(*testing.T) {
// 	return func(t *testing.T) {
// 		data, err := os.ReadFile("../../test/assets/data/csv/COINBASE_BTCUSD, 30.csv")
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		dp, err := dataprocessors.NewDataProcessor(csv.CsvProcessorName)
// 		if err != nil {
// 			t.Error(err)
// 		}

// 		measurements := map[string]string{
// 			"open":   "open",
// 			"high":   "high",
// 			"low":    "low",
// 			"close":  "close",
// 			"volume": "volume",
// 		}

// 		err = dp.Init(nil, nil, measurements, nil, nil)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		_, err = dp.OnData(data)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		record, err := dp.GetRecord()
// 		if err != nil {
// 			t.Error(err)
// 			return
// 		}

// 		newObservations, err := dataspace.ArrowToObservations(record)
// 		record.Release()
// 		if err != nil {
// 			t.Error(err)
// 			return
// 		}

// 		csv := strings.Builder{}
// 		epoch := time.Unix(1605312000, 0)
// 		timeCategoryNames := []string{"month", "dayofweek"}
// 		timeCategoryFields := spice_time.GenerateTimeCategoryFields(timeCategoryNames...)
// 		measurementNames := strings.Split("open,high,low,close,volume", ",")
// 		tags := make([]string, 0)

// 		actualPreviewCsv := getData(&csv, epoch, timeCategoryNames, timeCategoryFields, measurementNames, nil, tags, newObservations, 5)

// 		snapshotter.SnapshotT(t, actualPreviewCsv)
// 	}
// }

// func testGetDataWithTagsFunc() func(*testing.T) {
// 	return func(t *testing.T) {
// 		data, err := os.ReadFile("../../test/assets/data/csv/csv_data_with_tags.csv")
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		dp, err := dataprocessors.NewDataProcessor(csv.CsvProcessorName)
// 		if err != nil {
// 			t.Error(err)
// 		}

// 		measurements := map[string]string{
// 			"height": "h",
// 			"rating": "rating",
// 			"speed":  "speed",
// 			"target": "target",
// 		}

// 		tagSelectors := []string{
// 			"tags1",
// 			"tags2",
// 			"tags3",
// 			"_tags",
// 		}

// 		err = dp.Init(nil, nil, measurements, nil, tagSelectors)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		_, err = dp.OnData(data)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		record, err := dp.GetRecord()
// 		if err != nil {
// 			t.Error(err)
// 			return
// 		}

// 		newObservations, err := dataspace.ArrowToObservations(record)
// 		record.Release()
// 		if err != nil {
// 			t.Error(err)
// 			return
// 		}

// 		csv := strings.Builder{}
// 		epoch := time.Unix(1610057400, 0)
// 		timeCategoryNames := []string{}
// 		timeCategoryFields := spice_time.GenerateTimeCategoryFields(timeCategoryNames...)
// 		measurementNames := []string{"height", "rating", "speed", "target"}
// 		tags := []string{"tagA", "tagB", "tagC", "tagD"}

// 		actualPreviewCsv := getData(&csv, epoch, timeCategoryNames, timeCategoryFields, measurementNames, nil, tags, newObservations, 5)

// 		snapshotter.SnapshotT(t, actualPreviewCsv)
// 	}
// }

// func testGetDataWithCategoriesFunc() func(*testing.T) {
// 	return func(t *testing.T) {
// 		data, err := os.ReadFile("../../test/assets/data/json/event_stream_categories.json")
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		dp, err := dataprocessors.NewDataProcessor(json.JsonProcessorName)
// 		if err != nil {
// 			t.Error(err)
// 		}

// 		measurements := map[string]string{
// 			"duration":     "length_of_time",
// 			"guest_count":  "num_guests",
// 			"ticket_price": "ticket_price",
// 		}

// 		categories := map[string]string{
// 			"event_type":      "event_type",
// 			"target_audience": "target_audience",
// 		}

// 		tagsSelectors := []string{"tags"}

// 		err = dp.Init(nil, nil, measurements, categories, tagsSelectors)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		_, err = dp.OnData(data)
// 		if err != nil {
// 			t.Fatal(err)
// 		}

// 		record, err := dp.GetRecord()
// 		if err != nil {
// 			t.Error(err)
// 			return
// 		}

// 		newObservations, err := dataspace.ArrowToObservations(record)
// 		record.Release()
// 		if err != nil {
// 			t.Error(err)
// 			return
// 		}

// 		categoriesList := []*dataspace.CategoryInfo{
// 			{Name: "event_type", Values: []string{"dinner", "party", "dance", "concert", "football_game"}},
// 			{Name: "target_audience", Values: []string{"employees", "investors", "cohort_a"}},
// 		}

// 		csv := strings.Builder{}
// 		epoch := time.Unix(1610057400, 0)
// 		timeCategoryNames := []string{}
// 		timeCategoryFields := spice_time.GenerateTimeCategoryFields(timeCategoryNames...)
// 		measurementNames := []string{"duration", "guest_count", "ticket_price"}
// 		tags := []string{"tagA", "tagB", "tagC"}

// 		t.Log(measurementNames)
// 		getData(&csv, epoch, timeCategoryNames, timeCategoryFields, measurementNames, categoriesList, tags, newObservations, 5)

// 		snapshotter.SnapshotT(t, csv.String())
// 	}
// }
