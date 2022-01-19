package state

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow/go/v6/arrow"
	"github.com/apache/arrow/go/v6/arrow/array"
	"github.com/apache/arrow/go/v6/arrow/memory"
	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/data-components-contrib/dataconnectors/file"
	"github.com/spiceai/data-components-contrib/dataprocessors/csv"
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
		expectedOrigin := "test.path"
		fields := []arrow.Field{
			{Name: "time", Type: arrow.PrimitiveTypes.Int64},
			{Name: "id.i-1", Type: arrow.PrimitiveTypes.Float64},
			{Name: "id.i-2", Type: arrow.PrimitiveTypes.Float64},
			{Name: "measure.m-1", Type: arrow.PrimitiveTypes.Float64},
			{Name: "measure.m-2", Type: arrow.PrimitiveTypes.Float64},
			{Name: "measure.m-3", Type: arrow.PrimitiveTypes.Float64},
			{Name: "cat.c-1", Type: arrow.PrimitiveTypes.Float64},
			{Name: "cat.c-2", Type: arrow.PrimitiveTypes.Float64},
			{Name: "cat.c-3", Type: arrow.PrimitiveTypes.Float64},
		}
		pool := memory.NewGoAllocator()
		recordBuilder := array.NewRecordBuilder(pool, arrow.NewSchema(fields, nil))
		defer recordBuilder.Release()
		expectedRecord := recordBuilder.NewRecord()
		defer expectedRecord.Release()

		newState := NewState(expectedOrigin, expectedRecord)

		assert.Equal(t, expectedOrigin, newState.Origin(), "Path() not equal")
		assert.Equal(t, []string{"measure.m-1", "measure.m-2", "measure.m-3"}, newState.MeasurementNames(), "MeasurementNames() not equal")
	}
}

// Tests "GetState()"
func testGetStateFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		measurements := map[string]string{
			"coinbase.btcusd.price":       "coinbase.btcusd.price",
			"local.portfolio.usd_balance": "local.portfolio.usd_balance",
			"local.portfolio.btc_balance": "local.portfolio.btc_balance",
		}

		dp := csv.NewCsvProcessor()
		err := dp.Init(nil, nil, measurements, nil, nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualRecord, err := dp.GetRecord()
		if err != nil {
			t.Error(err)
			return
		}
		actualStates := GetStatesFromRecord(actualRecord)

		assert.Equal(t, 2, len(actualStates), "expected two state objects")
		assert.Equal(t, "coinbase.btcusd", actualStates[0].Origin(), "expected origin incorrect")

		fields := []arrow.Field{
			{Name: "time", Type: arrow.PrimitiveTypes.Int64},
			{Name: "measure.price", Type: arrow.PrimitiveTypes.Float64},
		}
		pool := memory.NewGoAllocator()
		recordBuilder := array.NewRecordBuilder(pool, arrow.NewSchema(fields, nil))
		defer recordBuilder.Release()
		recordBuilder.Field(0).(*array.Int64Builder).AppendValues([]int64{1626697480}, nil)
		recordBuilder.Field(1).(*array.Float64Builder).AppendValues([]float64{31232.709090909084}, nil)

		expectedRecord := recordBuilder.NewRecord()
		defer expectedRecord.Release()

		assert.Equal(t, actualStates[0].Record().NumRows(), int64(57), "number of observations incorrect")
		assert.True(t, array.RecordEqual(expectedRecord, actualStates[0].Record().NewSlice(0, 1)), "First Record not correct")
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

	measurements := map[string]string{
		"coinbase.btcusd.open":      "coinbase.btcusd.open",
		"bitthumb.btcusd.high":      "bitthumb.btcusd.high",
		"bitmex.btcusd.low":         "bitmex.btcusd.low",
		"coinbase_pro.btcusd.close": "coinbase_pro.btcusd.close",
		"local.btcusd.volume":       "local.btcusd.volume",
	}
	tags := []string{"coinbase.btcusd._tags", "local.btcusd._tags"}
	dp := csv.NewCsvProcessor()
	err = dp.Init(nil, nil, measurements, nil, tags)
	assert.NoError(t, err)

	_, err = dp.OnData(data)
	assert.NoError(t, err)

	actualRecord, err := dp.GetRecord()
	if err != nil {
		t.Error(err)
		return
	}
	actualStates := GetStatesFromRecord(actualRecord)

	assert.Equal(t, 5, len(actualStates), "expected five state objects")

	assert.Equal(t, "bitmex.btcusd", actualStates[0].Origin(), "expected origin incorrect")
	assert.Equal(t, "bitthumb.btcusd", actualStates[1].Origin(), "expected origin incorrect")
	assert.Equal(t, "coinbase.btcusd", actualStates[2].Origin(), "expected origin incorrect")
	assert.Equal(t, "coinbase_pro.btcusd", actualStates[3].Origin(), "expected origin incorrect")
	assert.Equal(t, "local.btcusd", actualStates[4].Origin(), "expected origin incorrect")

	fields := []arrow.Field{
		{Name: "time", Type: arrow.PrimitiveTypes.Int64},
		{Name: "measure.low", Type: arrow.PrimitiveTypes.Float64},
		{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)},
	}
	pool := memory.NewGoAllocator()
	recordBuilder := array.NewRecordBuilder(pool, arrow.NewSchema(fields, nil))
	defer recordBuilder.Release()
	recordBuilder.Field(0).(*array.Int64Builder).AppendValues([]int64{1605312000}, nil)
	recordBuilder.Field(1).(*array.Float64Builder).AppendValues([]float64{16240}, nil)
	listBuilder := recordBuilder.Field(2).(*array.ListBuilder)
	valueBuilder := listBuilder.ValueBuilder().(*array.StringBuilder)
	listBuilder.Append(true)
	valueBuilder.Append("elon_tweet")
	valueBuilder.Append("market_open")
	valueBuilder.Append("bought_1")
	valueBuilder.Append("tag_2")

	expectedRecord := recordBuilder.NewRecord()
	defer expectedRecord.Release()

	assert.True(t, array.RecordEqual(expectedRecord, actualStates[0].Record().NewSlice(0, 1)), "First Record not correct")
	assert.Equal(t, actualStates[0].Record().NumRows(), int64(5), "record length incorrect")

	testTime := time.Unix(1610057400, 0)
	testTime = testTime.UTC()
	for _, state := range actualStates {
		state.Time = testTime
	}

	snapshotter.SnapshotT(t, actualStates)
}

func TestGetStateIdentifiers(t *testing.T) {
	data, err := os.ReadFile("../../test/assets/data/csv/e2e_csv_data_with_tags_2.csv")
	if err != nil {
		t.Fatal(err.Error())
	}

	if len(data) == 0 {
		t.Fatal("no data")
	}

	identifiers := map[string]string{
		"event.data.event_id": "event.data.event_id",
	}
	measurements := map[string]string{
		"event.data.speed":  "event.data.speed",
		"event.data.target": "event.data.target",
	}
	categories := map[string]string{
		"event.data.rating": "event.data.rating",
	}
	dp := csv.NewCsvProcessor()
	err = dp.Init(nil, identifiers, measurements, categories, nil)
	assert.NoError(t, err)

	_, err = dp.OnData(data)
	assert.NoError(t, err)

	actualRecord, err := dp.GetRecord()
	if err != nil {
		t.Error(err)
		return
	}
	actualStates := GetStatesFromRecord(actualRecord)

	assert.Equal(t, 1, len(actualStates), "expected one state object")
	assert.Equal(t, "event.data", actualStates[0].Origin(), "expected origin incorrect")

	fields := []arrow.Field{
		{Name: "time", Type: arrow.PrimitiveTypes.Int64},
		{Name: "id.event_id", Type: arrow.BinaryTypes.String},
		{Name: "measure.speed", Type: arrow.PrimitiveTypes.Float64},
		{Name: "measure.target", Type: arrow.PrimitiveTypes.Float64},
		{Name: "cat.rating", Type: arrow.BinaryTypes.String},
	}
	pool := memory.NewGoAllocator()
	recordBuilder := array.NewRecordBuilder(pool, arrow.NewSchema(fields, nil))
	defer recordBuilder.Release()
	recordBuilder.Field(0).(*array.Int64Builder).AppendValues([]int64{1611205740}, nil)
	recordBuilder.Field(1).(*array.StringBuilder).AppendValues([]string{"3"}, nil)
	recordBuilder.Field(2).(*array.Float64Builder).AppendValues([]float64{15}, nil)
	recordBuilder.Field(3).(*array.Float64Builder).AppendValues([]float64{1}, nil)
	recordBuilder.Field(4).(*array.StringBuilder).AppendValues([]string{"10"}, nil)

	expectedRecord := recordBuilder.NewRecord()
	defer expectedRecord.Release()

	assert.True(t, array.RecordEqual(expectedRecord, actualStates[0].Record().NewSlice(0, 1)), "First Record not correct")
	assert.Equal(t, actualStates[0].Record().NumRows(), int64(5), "record length incorrect")

	testTime := time.Unix(1610057400, 0)
	testTime = testTime.UTC()
	for _, state := range actualStates {
		state.Time = testTime
	}

	snapshotter.SnapshotT(t, actualStates)
}

// Tests "GetState()" called twice
func testGetStateTwiceFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		measurements := map[string]string{
			"coinbase.btcusd.price":       "coinbase.btcusd.price",
			"local.portfolio.usd_balance": "local.portfolio.usd_balance",
			"local.portfolio.btc_balance": "local.portfolio.btc_balance",
		}
		dp := csv.NewCsvProcessor()
		err := dp.Init(nil, nil, measurements, nil, nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

		actualRecord, err := dp.GetRecord()
		if err != nil {
			t.Error(err)
			return
		}
		actualStates := GetStatesFromRecord(actualRecord)

		assert.Equal(t, 2, len(actualStates), "expected two state objects")
		assert.Equal(t, "coinbase.btcusd", actualStates[0].Origin(), "expected origin incorrect")

		fields := []arrow.Field{
			{Name: "time", Type: arrow.PrimitiveTypes.Int64},
			{Name: "measure.price", Type: arrow.PrimitiveTypes.Float64},
		}
		pool := memory.NewGoAllocator()
		recordBuilder := array.NewRecordBuilder(pool, arrow.NewSchema(fields, nil))
		defer recordBuilder.Release()
		recordBuilder.Field(0).(*array.Int64Builder).AppendValues([]int64{1626697480}, nil)
		recordBuilder.Field(1).(*array.Float64Builder).AppendValues([]float64{31232.709090909084}, nil)

		expectedRecord := recordBuilder.NewRecord()
		defer expectedRecord.Release()

		assert.True(t, array.RecordEqual(expectedRecord, actualStates[0].Record().NewSlice(0, 1)), "First Record not correct")
		assert.Equal(t, actualStates[0].Record().NumRows(), int64(57), "number of observations incorrect")
	}
}
