package state

import (
	"os"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/data-components-contrib/dataconnectors/file"
	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/state"))

func TestContext(t *testing.T) {
	t.Run("NewState() - NewState and getters", testNewState())
}

func TestGetStateFromCsv(t *testing.T) {
	globalFileConnector := file.NewFileConnector()

	var wg sync.WaitGroup
	var globalData []byte
	err := globalFileConnector.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
		globalData = data
		wg.Done()
		return nil, nil
	})
	if err != nil {
		t.Fatal(err.Error())
	}
	wg.Add(1)

	epoch := time.Unix(1605312000, 0)
	period := 7 * 24 * time.Hour
	interval := time.Hour

	err = globalFileConnector.Init(epoch, period, interval, map[string]string{
		"path":  "../../test/assets/data/csv/trader_input.csv",
		"watch": "false",
	})
	if err != nil {
		t.Fatal(err.Error())
	}

	wg.Wait()

	t.Run("GetState()", testGetStateFunc(globalData))
	t.Run("GetState() called twice", testGetStateTwiceFunc(globalData))
}

// Tests NewState() creates State correctly with valid getter values
func testNewState() func(*testing.T) {
	return func(t *testing.T) {
		expectedPath := "test.path"
		expectedMeasurementsNames := []string{"m-1", "m-2", "m-3"}
		expectedCategoryNames := []string{"c-1", "c-2", "c-3"}
		expectedTags := []string{}
		expectedObservations := []observations.Observation{}

		newState := NewState(expectedPath, expectedMeasurementsNames, expectedCategoryNames, expectedTags, expectedObservations)

		assert.Equal(t, expectedPath, newState.Path(), "Path() not equal")
		assert.Equal(t, expectedMeasurementsNames, newState.MeasurementsNames(), "MeasurementNames() not equal")

		expectedFqMeasurementNames := []string{"test.path.m-1", "test.path.m-2", "test.path.m-3"}
		assert.Equal(t, expectedFqMeasurementNames, newState.FqMeasurementsNames(), "FqMeasurementsNames() not equal")
	}
}

// Tests "GetState()"
func testGetStateFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		validMeasurementNames := []string{"coinbase.btcusd.price", "local.portfolio.usd_balance", "local.portfolio.btc_balance"}

		actualState, err := GetStateFromCsv(nil, validMeasurementNames, nil, data)
		if err != nil {
			t.Error(err)
			return
		}

		assert.Equal(t, 1, len(actualState), "expected two state objects")

		assert.Equal(t, "coinbase.btcusd", actualState[0].Path(), "expected path incorrect")

		expectedFirstObservation := observations.Observation{
			Time: 1626697480,
			Measurements: map[string]float64{
				"price": 31232.709090909084,
			},
		}

		actualObservations := actualState[0].Observations()
		assert.Len(t, actualObservations, 57, "number of observations incorrect")
		assert.Equal(t, expectedFirstObservation, actualState[0].Observations()[0], "First Observation not correct")
	}
}

func TestGetStateWithTagsFunc(t *testing.T) {
	data, err := os.ReadFile("../../test/assets/data/csv/global_tag_data.csv")
	if err != nil {
		t.Fatal(err.Error())
	}
	
	if len(data) == 0 {
		t.Fatal("no data")
	}

	validMeasurementNames := []string{"coinbase.btcusd.open", "bitthumb.btcusd.high", "bitmex.btcusd.low", "coinbase_pro.btcusd.close", "local.btcusd.volume"}

	actualState, err := GetStateFromCsv(nil, validMeasurementNames, nil, data)
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t, 5, len(actualState), "expected five state objects")

	sort.Slice(actualState, func(i, j int) bool {
		return actualState[i].Path() < actualState[j].Path()
	})

	assert.Equal(t, "bitmex.btcusd", actualState[0].Path(), "expected path incorrect")
	assert.Equal(t, "bitthumb.btcusd", actualState[1].Path(), "expected path incorrect")
	assert.Equal(t, "coinbase.btcusd", actualState[2].Path(), "expected path incorrect")
	assert.Equal(t, "coinbase_pro.btcusd", actualState[3].Path(), "expected path incorrect")
	assert.Equal(t, "local.btcusd", actualState[4].Path(), "expected path incorrect")

	expectedFirstObservation := observations.Observation{
		Time: 1605312000,
		Measurements: map[string]float64{
			"low": 16240,
		},
	}

	actualObservations := actualState[0].Observations()
	assert.Equal(t, expectedFirstObservation, actualState[0].Observations()[0], "First Observation not correct")
	assert.Equal(t, 5, len(actualObservations), "number of observations incorrect")

	testTime := time.Unix(1610057400, 0)
	testTime = testTime.UTC()
	for _, state := range actualState {
		state.Time = testTime
	}

	snapshotter.SnapshotT(t, actualState)
}

func TestGetStateIdentifiers(t *testing.T) {
	data, err := os.ReadFile("../../test/assets/data/csv/e2e_csv_data_with_tags_2.csv")
	if err != nil {
		t.Fatal(err.Error())
	}
	
	if len(data) == 0 {
		t.Fatal("no data")
	}

	validIdentifierNames := []string{"event.data.event_id"}
	validMeasurementNames := []string{"event.data.speed","event.data.target"}
	validCategoryNames := []string{"event.data.rating"}

	actualState, err := GetStateFromCsv(validIdentifierNames, validMeasurementNames, validCategoryNames, data)
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t, 1, len(actualState), "expected one state object")

	assert.Equal(t, "event.data", actualState[0].Path(), "expected path incorrect")

	expectedFirstObservation := observations.Observation{
		Time:1611205740,
		Identifiers:map[string]string{"event_id":"3"},
		Measurements:map[string]float64{"speed":15, "target":1},
		Categories:map[string]string{"rating":"10"},
	}

	actualObservations := actualState[0].Observations()
	assert.Equal(t, expectedFirstObservation, actualState[0].Observations()[0], "First Observation not correct")
	assert.Equal(t, 5, len(actualObservations), "number of observations incorrect")

	testTime := time.Unix(1610057400, 0)
	testTime = testTime.UTC()
	for _, state := range actualState {
		state.Time = testTime
	}

	snapshotter.SnapshotT(t, actualState)
}

// Tests "GetState()" called twice
func testGetStateTwiceFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		validMeasurementNames := []string{"coinbase.btcusd.price", "local.portfolio.usd_balance", "local.portfolio.btc_balance"}

		actualState, err := GetStateFromCsv(nil, validMeasurementNames, nil, data)
		if err != nil {
			t.Error(err)
			return
		}

		assert.Equal(t, "coinbase.btcusd", actualState[0].Path(), "expected path incorrect")

		expectedFirstObservation := observations.Observation{
			Time: 1626697480,
			Measurements: map[string]float64{
				"price": 31232.709090909084,
			},
		}

		actualObservations := actualState[0].Observations()
		assert.Equal(t, expectedFirstObservation, actualState[0].Observations()[0], "First Observation not correct")
		assert.Equal(t, 57, len(actualObservations), "number of observations incorrect")
	}
}

func TestProcessCsvHeaders(t *testing.T) {
	headers := []string{"time", "coinbase.btcusd.transaction_id", "coinbase.btcusd.open", "coinbase.btcusd._tags", "bitthumb.btcusd.high", "bitmex.btcusd.ticker_id", "bitmex.btcusd.low", "coinbase_pro.btcusd.close", "local.btcusd.volume", "local.btcusd.type", "local.btcusd._tags"}
	validIdentifierNames := []string{"coinbase.btcusd.transaction_id", "bitmex.btcusd.ticker_id"}
	validMeasurementNames := []string{"coinbase.btcusd.open", "bitmex.btcusd.low", "local.btcusd.volume"}
	validCategoryNames := []string{"local.btcusd.type"}

	dsPathsMap, colToDsPath, colToIdentifierName, colToMeasurementName, colToCategoryName, tagsCol, err := processCsvHeaders(headers, validIdentifierNames, validMeasurementNames, validCategoryNames)
	assert.NoError(t, err)

	expectedDsPathsMap := map[string]bool(map[string]bool{"bitmex.btcusd": true, "coinbase.btcusd": true, "local.btcusd": true})
	assert.Equal(t, expectedDsPathsMap, dsPathsMap)

	expectedColToDsPath := []string([]string{"coinbase.btcusd", "coinbase.btcusd", "coinbase.btcusd", "", "bitmex.btcusd", "bitmex.btcusd", "", "local.btcusd", "local.btcusd", "local.btcusd"})
	assert.Equal(t, expectedColToDsPath, colToDsPath)

	expectedColToIdentifierName := []string([]string{"transaction_id", "", "", "", "ticker_id", "", "", "", "", ""})
	assert.Equal(t, expectedColToIdentifierName, colToIdentifierName)
	
	expectedColToMeasurementName := []string([]string{"", "open", "", "", "", "low", "", "volume", "", ""})
	assert.Equal(t, expectedColToMeasurementName, colToMeasurementName)

	expectedColToCategoryName := []string([]string{"", "", "", "", "", "", "", "", "type", ""})
	assert.Equal(t, expectedColToCategoryName, colToCategoryName)

	expectedTagsCol := []string([]string{"", "", "coinbase.btcusd._tags", "", "", "", "", "", "", "local.btcusd._tags"})
	assert.Equal(t, expectedTagsCol, tagsCol)
}

func TestProcessCsvHeadersNoTags(t *testing.T) {
	headers := []string{"time", "coinbase.btcusd.transaction_id", "coinbase.btcusd.open", "bitthumb.btcusd.high", "bitmex.btcusd.ticker_id", "bitmex.btcusd.low", "coinbase_pro.btcusd.close", "local.btcusd.volume", "local.btcusd.type"}
	validIdentifierNames := []string{"coinbase.btcusd.transaction_id", "bitmex.btcusd.ticker_id"}
	validMeasurementNames := []string{"coinbase.btcusd.open", "bitmex.btcusd.low", "local.btcusd.volume"}
	validCategoryNames := []string{"local.btcusd.type"}

	dsPathsMap, colToDsPath, colToIdentifierName, colToMeasurementName, colToCategoryName, tagsCol, err := processCsvHeaders(headers, validIdentifierNames, validMeasurementNames, validCategoryNames)
	assert.NoError(t, err)

	expectedDsPathsMap := map[string]bool(map[string]bool{"bitmex.btcusd": true, "coinbase.btcusd": true, "local.btcusd": true})
	assert.Equal(t, expectedDsPathsMap, dsPathsMap)

	expectedColToDsPath := []string([]string{"coinbase.btcusd", "coinbase.btcusd", "", "bitmex.btcusd", "bitmex.btcusd", "", "local.btcusd", "local.btcusd"})
	assert.Equal(t, expectedColToDsPath, colToDsPath)

	expectedColToIdentifierName := []string([]string{"transaction_id", "", "", "ticker_id", "", "", "", ""})
	assert.Equal(t, expectedColToIdentifierName, colToIdentifierName)

	expectedColToMeasurementName := []string([]string{"", "open", "", "", "low", "", "volume", ""})
	assert.Equal(t, expectedColToMeasurementName, colToMeasurementName)

	expectedColToCategoryName := []string([]string{"", "", "", "", "", "", "", "type"})
	assert.Equal(t, expectedColToCategoryName, colToCategoryName)

	expectedTagsCol := []string([]string{"", "", "", "", "", "", "", ""})
	assert.Equal(t, expectedTagsCol, tagsCol)
}

func TestGetDsPathToDataMap(t *testing.T) {
	dsPathsMap := map[string]bool(map[string]bool{"bitmex.btcusd": true, "coinbase.btcusd": true, "local.btcusd": true})
	colToDsPath := []string([]string{"coinbase.btcusd", "", "bitmex.btcusd", "", "local.btcusd", "local.btcusd"})
	colToIdentifierName := []string([]string{"", "transaction_id", "", "", "", ""})
	colToMeasurementName := []string([]string{"open", "", "low", "", "volume", ""})
	colToCategoryName := []string([]string{"", "", "", "", "", "type"})

	dsPathToDataMap := getDsPathToDataMap(len(dsPathsMap), colToDsPath, colToIdentifierName, colToMeasurementName, colToCategoryName)

	snapshotter.SnapshotT(t, dsPathToDataMap)
}

func TestGetFieldNames(t *testing.T) {
	names := []string{"a", "b", "c"}
	fqNames, namesMap := getFieldNames("my.test.path", names)

	expectedFqNames := []string{"my.test.path.a", "my.test.path.b", "my.test.path.c"}
	assert.Equal(t, expectedFqNames, fqNames)

	assert.Len(t, namesMap, len(names))

	for i, n := range names {
		assert.Equal(t, namesMap[n], expectedFqNames[i])
	}
}
