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
	"github.com/stretchr/testify/assert"
)

func TestObservations(t *testing.T) {
	t.Run("getData() - All headers with preview", testGetCsvAllHeadersWithPreviewFunc())
	t.Run("getData() - Select headers with preview", testGetCsvSelectHeadersWithPreviewFunc())
	t.Run("getData() - With tags", testGetDataWithTagsFunc())
	t.Run("getData() - With categories", testGetDataWithCategoriesFunc())
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

		err = dp.Init(nil, nil, nil)
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
		headers := strings.Split(headerLine, ",")
		tags := make([]string, 0)

		actualPreviewCsv := getData(&csv, epoch, headers, tags, nil, newObservations, 5)

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

		err = dp.Init(nil, nil, nil)
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
		headers := strings.Split("open,high,low,close,volume", ",")
		tags := make([]string, 0)

		actualPreviewCsv := getData(&csv, epoch, headers, tags, nil, newObservations, 5)

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

		err = dp.Init(nil, nil, nil)
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
		fieldNames := []string{"eventId", "height", "rating", "speed", "target"}
		tags := []string{"tagA", "tagB", "tagC"}

		actualPreviewCsv := getData(&csv, epoch, fieldNames, tags, nil, newObservations, 5)

		expectedPreviewCsv := `1610057400,1,10,10,15,1,1,1,1
1610057800,2,20,11,30,2,1,0,0
1610058200,4,30,12,45,3,1,0,1
1610058600,8,40,13,60,4,0,1,1
1610059000,16,50,14,75,5,0,0,1
`

		assert.Equal(t, expectedPreviewCsv, actualPreviewCsv, "preview csv did not match")
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

		err = dp.Init(nil, measurements, categories)
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

		categoriesList := []*dataspace.Category{
			{Name: "event_type", Values: []string{"dinner", "party", "dance", "concert", "football_game"}},
			{Name: "target_audience", Values: []string{"employees", "investors", "cohort_a"}},
		}

		csv := strings.Builder{}
		epoch := time.Unix(1610057400, 0)
		fieldNames := []string{"duration", "guest_count", "ticket_price", "event_type", "target_audience"}
		tags := []string{"tagA", "tagB", "tagC"}

		t.Log(fieldNames)
		getData(&csv, epoch, fieldNames, tags, categoriesList, newObservations, 5)

		snapshotter.SnapshotT(t, csv.String())
	}
}
