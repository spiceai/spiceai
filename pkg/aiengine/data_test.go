package aiengine

import (
	"io/ioutil"
	"strings"
	"testing"
	"time"

	"github.com/spiceai/data-components-contrib/dataprocessors"
	"github.com/spiceai/data-components-contrib/dataprocessors/csv"
	"github.com/stretchr/testify/assert"
)

func TestObservations(t *testing.T) {
	t.Run("getData() - All headers with preview", testGetCsvAllHeadersWithPreviewFunc())
	t.Run("getData() - Select headers with preview", testGetCsvSelectHeadersWithPreviewFunc())
}

// Tests "GetCsv() - All headers with preview
func testGetCsvAllHeadersWithPreviewFunc() func(*testing.T) {
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

		csv := strings.Builder{}
		epoch := time.Unix(1605312001, 0)
		headerLine := "open,high,low,close,volume"
		headers := strings.Split(headerLine, ",")

		actualPreviewCsv := getData(&csv, epoch, headers, newObservations, 5)

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

		csv := strings.Builder{}
		epoch := time.Unix(1605312000, 0)
		headers := strings.Split("open,high,low,close,volume", ",")

		actualPreviewCsv := getData(&csv, epoch, headers, newObservations, 5)

		expectedPreviewCsv := `1605312000,16339.56,16339.6,16240,16254.51,274.42607
1605313800,16256.42,16305,16248.6,16305,110.91971
1605315600,16303.88,16303.88,16210.99,16222.16,231.64805
1605317400,16221.78,16246.92,16200.01,16214.15,135.86161
1605319200,16214.26,16234,16175.62,16223.08,164.32561
`

		assert.Equal(t, expectedPreviewCsv, actualPreviewCsv, "preview csv did not match")
	}
}
