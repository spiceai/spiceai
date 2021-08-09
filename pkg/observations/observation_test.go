package observations_test

import (
	"bytes"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/spice/pkg/csv"
	"github.com/spiceai/spice/pkg/observations"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/observations"))

func TestObservations(t *testing.T) {
	t.Run("GetCsv() - All headers with preview", testGetCsvAllHeadersWithPreviewFunc())
	t.Run("GetCsv() - Select headers with preview", testGetCsvSelectHeadersWithPreviewFunc())
}

// Tests "GetCsv() - All headers with preview
func testGetCsvAllHeadersWithPreviewFunc() func(*testing.T) {
	return func(t *testing.T) {
		csvFilePath := "../../test/assets/data/csv/COINBASE_BTCUSD, 30.csv"
		csvData, err := ioutil.ReadFile(csvFilePath)
		if err != nil {
			t.Error(err)
			return
		}

		reader := bytes.NewReader(csvData)

		data, err := csv.ProcessCsv(reader)
		if err != nil {
			t.Error(err)
			return
		}

		headerLine := "open,high,low,close,volume"
		headers := strings.Split(headerLine, ",")

		actualCsvNoHeaders, actualPreviewCsv := observations.GetCsv(headers, data, 5)

		expectedPreviewCsv := `1605312000,16339.56,16339.6,16240,16254.51,274.42607
1605313800,16256.42,16305,16248.6,16305,110.91971
1605315600,16303.88,16303.88,16210.99,16222.16,231.64805
1605317400,16221.78,16246.92,16200.01,16214.15,135.86161
1605319200,16214.26,16234,16175.62,16223.08,164.32561
`

		assert.Equal(t, expectedPreviewCsv, actualPreviewCsv, "preview csv did not match")

		snapshotter.SnapshotT(t, actualCsvNoHeaders)
	}
}

// Tests "GetCsv() - All headers with preview
func testGetCsvSelectHeadersWithPreviewFunc() func(*testing.T) {
	return func(t *testing.T) {
		csvFilePath := "../../test/assets/data/csv/COINBASE_BTCUSD, 30.csv"
		csvData, err := ioutil.ReadFile(csvFilePath)
		if err != nil {
			t.Error(err)
			return
		}

		reader := bytes.NewReader(csvData)

		data, err := csv.ProcessCsv(reader)
		if err != nil {
			t.Error(err)
			return
		}

		headers := strings.Split("open,high,low,close,volume", ",")

		_, actualPreviewCsv := observations.GetCsv(headers, data, 5)

		expectedPreviewCsv := `1605312000,16339.56,16339.6,16240,16254.51,274.42607
1605313800,16256.42,16305,16248.6,16305,110.91971
1605315600,16303.88,16303.88,16210.99,16222.16,231.64805
1605317400,16221.78,16246.92,16200.01,16214.15,135.86161
1605319200,16214.26,16234,16175.62,16223.08,164.32561
`

		assert.Equal(t, expectedPreviewCsv, actualPreviewCsv, "preview csv did not match")
	}
}
