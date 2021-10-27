package dataspace

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/spf13/viper"
	"github.com/spiceai/spiceai/pkg/spec"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/dataspace"))

func TestDataSource(t *testing.T) {

	manifestsToTest := []string{"trader", "event-tags", "event-categories"}

	for _, manifestName := range manifestsToTest {
		v := viper.New()
		v.AddConfigPath("../../test/assets/pods/manifests")
		v.SetConfigName(manifestName)
		v.SetConfigType("yaml")

		err := v.ReadInConfig()
		if err != nil {
			t.Error(err)
		}

		var datasources []spec.DataspaceSpec
		err = v.UnmarshalKey("dataspaces", &datasources)
		if err != nil {
			t.Error(err)
		}

		numDataSourcesActual := len(datasources)
		if numDataSourcesActual == 0 {
			t.Errorf("Expected > 0, got %d", numDataSourcesActual)
		}

		for _, dsSpec := range datasources {
			dsName := fmt.Sprintf("%s/%s", dsSpec.From, dsSpec.Name)

			t.Run(fmt.Sprintf("NewDataspace() - %s", dsName), testNewDataspaceFunc(dsSpec))
			t.Run(fmt.Sprintf("Actions() - %s", dsName), testActionsFunc(dsSpec))
			t.Run(fmt.Sprintf("Measurements() - %s", dsName), testMeasurementsFunc(dsSpec))
			t.Run(fmt.Sprintf("MeasurementNames() - %s", dsName), testMeasurementNamesFunc(dsSpec))
			t.Run(fmt.Sprintf("ActionNames() - %s", dsName), testActionNamesFunc(dsSpec))
			t.Run(fmt.Sprintf("Laws() - %s", dsName), testLawsFunc(dsSpec))
			t.Run(fmt.Sprintf("getMeasurements() - %s", dsName), testGetMeasurementsFunc(dsSpec))
			t.Run(fmt.Sprintf("getCategories() - %s", dsName), testGetCategoriesFunc(dsSpec))
			t.Run(fmt.Sprintf("getTags() - %s", dsName), testGetTagsFunc(dsSpec))
		}
	}
}

func testGetMeasurementsFunc(dsSpec spec.DataspaceSpec) func(*testing.T) {
	return func(t *testing.T) {
		actualMeasurements, actualMeasurementSelectors := getMeasurements(dsSpec)

		err := snapshotter.SnapshotMulti(dsSpec.Name+"_measurements", actualMeasurements)
		if err != nil {
			t.Fatal(err)
		}

		err = snapshotter.SnapshotMulti(dsSpec.Name+"_measurement_selectors", actualMeasurementSelectors)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func testGetCategoriesFunc(dsSpec spec.DataspaceSpec) func(*testing.T) {
	return func(t *testing.T) {
		actualCategories, actualCategorySelectors := getCategories(dsSpec)

		err := snapshotter.SnapshotMulti(dsSpec.Name+"_categories", actualCategories)
		if err != nil {
			t.Fatal(err)
		}

		err = snapshotter.SnapshotMulti(dsSpec.Name+"_category_selectors", actualCategorySelectors)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func testGetTagsFunc(dsSpec spec.DataspaceSpec) func(*testing.T) {
	return func(t *testing.T) {
		tags, fqTags := getTags(dsSpec)

		err := snapshotter.SnapshotMulti(dsSpec.Name+"_tags", tags)
		if err != nil {
			t.Fatal(err)
		}

		err = snapshotter.SnapshotMulti(dsSpec.Name+"_fqtags", fqTags)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Tests Dataspace creation from DataspaceSpec
func testNewDataspaceFunc(dsSpec spec.DataspaceSpec) func(*testing.T) {
	return func(t *testing.T) {
		ds, err := NewDataspace(dsSpec)
		if err != nil {
			t.Error(err)
		}

		expectedFQName := fmt.Sprintf("%s/%s", dsSpec.From, dsSpec.Name)
		actualFQName := ds.Name()
		if expectedFQName != actualFQName {
			t.Errorf("Expected '%s', got '%s'", expectedFQName, actualFQName)
		}
	}
}

// Tests Actions() getter
func testActionsFunc(dsSpec spec.DataspaceSpec) func(*testing.T) {
	return func(t *testing.T) {
		ds, err := NewDataspace(dsSpec)
		if err != nil {
			t.Error(err)
		}

		actual := ds.Actions()

		var expected map[string]string

		switch ds.Name() {
		case "local/portfolio":
			expected = map[string]string{
				"local.portfolio.buy":  "local.portfolio.usd_balance -= args.price\nlocal.portfolio.btc_balance += 1.1",
				"local.portfolio.sell": "local.portfolio.usd_balance += args.price\nlocal.portfolio.btc_balance -= 1",
			}
		case "event/data":
			fallthrough
		case "event/stream":
			fallthrough
		case "coinbase/btcusd":
			expected = make(map[string]string)
		}
		assert.Equal(t, expected, actual)
	}
}

// Tests Measurements() getter
func testMeasurementsFunc(dsSpec spec.DataspaceSpec) func(*testing.T) {
	return func(t *testing.T) {
		ds, err := NewDataspace(dsSpec)
		if err != nil {
			t.Error(err)
		}

		actual := ds.Measurements()

		err = snapshotter.SnapshotMulti(strings.ReplaceAll(ds.Name(), "/", "_"), actual)
		if err != nil {
			t.Fatal(err)
		}
	}
}

// Tests MeasurementNames() getter
func testMeasurementNamesFunc(dsSpec spec.DataspaceSpec) func(*testing.T) {
	return func(t *testing.T) {
		ds, err := NewDataspace(dsSpec)
		if err != nil {
			t.Error(err)
		}

		actual := ds.MeasurementNameMap()

		var expected map[string]string

		switch ds.Name() {
		case "local/portfolio":
			expected = map[string]string{
				"usd_balance": "local.portfolio.usd_balance",
				"btc_balance": "local.portfolio.btc_balance",
			}
		case "event/data":
			expected = map[string]string{
				"eventId": "event.data.eventId",
				"height":  "event.data.height",
				"rating":  "event.data.rating",
				"speed":   "event.data.speed",
				"target":  "event.data.target",
			}
		case "event/stream":
			expected = map[string]string{
				"duration":     "event.stream.duration",
				"guest_count":  "event.stream.guest_count",
				"ticket_price": "event.stream.ticket_price",
			}
		case "coinbase/btcusd":
			expected = map[string]string{
				"close": "coinbase.btcusd.close",
			}
		}

		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("Expected:\n%v\nGot:\n%v", expected, actual)
		}
	}
}

// Tests ActionNames() getter
func testActionNamesFunc(dsSpec spec.DataspaceSpec) func(*testing.T) {
	return func(t *testing.T) {
		ds, err := NewDataspace(dsSpec)
		if err != nil {
			t.Error(err)
		}

		actual := ds.ActionNames()

		var expected map[string]string

		switch ds.Name() {
		case "local/portfolio":
			expected = map[string]string{
				"buy":  "local.portfolio.buy",
				"sell": "local.portfolio.sell",
			}
		case "event/data":
			fallthrough
		case "event/stream":
			fallthrough
		case "coinbase/btcusd":
			expected = make(map[string]string)
		}

		assert.Equal(t, expected, actual)
	}
}

// Tests Laws() getter
func testLawsFunc(dsSpec spec.DataspaceSpec) func(*testing.T) {
	return func(t *testing.T) {
		ds, err := NewDataspace(dsSpec)
		if err != nil {
			t.Error(err)
		}

		actual := ds.Laws()

		var expected []string

		switch ds.Name() {
		case "local/portfolio":
			expected = []string{
				"local.portfolio.usd_balance >= 0",
				"local.portfolio.btc_balance >= 0",
			}
		case "coinbase/btcusd":
			// No laws
		case "event/stream":
			// No laws
		}

		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("Expected:\n%v\nGot:\n%v", expected, actual)
		}
	}
}
