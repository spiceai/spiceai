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
	globalDataTags, err := os.ReadFile("../../test/assets/data/csv/global_tag_data.csv")
	if err != nil {
		t.Fatal(err.Error())
	}

	globalFileConnector := file.NewFileConnector()

	var wg sync.WaitGroup
	var globalData []byte
	err = globalFileConnector.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
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
	t.Run("GetState() with tags", testGetStateTagsFunc(globalDataTags))
	t.Run("GetState() called twice", testGetStateTwiceFunc(globalData))
	t.Run("getColumnMappings()", testgetColumnMappingsFunc())
}

// Tests NewState() creates State correctly with valid getter values
func testNewState() func(*testing.T) {
	return func(t *testing.T) {
		expectedPath := "test.path"
		expectedFieldNames := []string{"field1", "field2", "field3"}
		expectedTags := []string{}
		expectedObservations := []observations.Observation{}

		newState := NewState(expectedPath, expectedFieldNames, expectedTags, expectedObservations)

		assert.Equal(t, expectedPath, newState.Path(), "Path() not equal")
		assert.Equal(t, expectedFieldNames, newState.MeasurementsNames(), "FieldNames() not equal")

		expectedFields := []string{"test.path.field1", "test.path.field2", "test.path.field3"}

		assert.Equal(t, expectedFields, newState.FqMeasurementsNames(), "Fields() not equal")
	}
}

// Tests "GetState()"
func testGetStateFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		actualState, err := GetStateFromCsv(nil, data)
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
			Measurements: map[string]float64{
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

// Tests "GetState()" with tag data
func testGetStateTagsFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		actualState, err := GetStateFromCsv(nil, data)
		if err != nil {
			t.Error(err)
			return
		}

		assert.Equal(t, 5, len(actualState), "expected two state objects")

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
}

// Tests "GetState()" called twice
func testGetStateTwiceFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		actualState, err := GetStateFromCsv(nil, data)
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
			Measurements: map[string]float64{
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
