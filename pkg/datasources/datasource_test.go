package datasources

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/spf13/viper"
	"github.com/spiceai/spice/pkg/spec"
)

func TestDataSource(t *testing.T) {

	manifestsToTest := []string{"trader", "cartpole-v1"}

	for _, manifestName := range manifestsToTest {
		v := viper.New()
		v.AddConfigPath("../../test/assets/pods/manifests")
		v.SetConfigName(manifestName)
		v.SetConfigType("yaml")

		err := v.ReadInConfig()
		if err != nil {
			t.Error(err)
		}

		var datasources []spec.DataSourceSpec
		err = v.UnmarshalKey("datasources", &datasources)
		if err != nil {
			t.Error(err)
		}

		numDataSourcesActual := len(datasources)
		if numDataSourcesActual == 0 {
			t.Errorf("Expected > 0, got %d", numDataSourcesActual)
		}

		for _, dsSpec := range datasources {
			dsName := fmt.Sprintf("%s/%s", dsSpec.From, dsSpec.Name)

			t.Run(fmt.Sprintf("NewDataSource() - %s", dsName), testNewDataSourceFunc(dsSpec))
			t.Run(fmt.Sprintf("Actions() - %s", dsName), testActionsFunc(dsSpec))
			t.Run(fmt.Sprintf("Fields() - %s", dsName), testFieldsFunc(dsSpec))
			t.Run(fmt.Sprintf("FieldNames() - %s", dsName), testFieldNamesFunc(dsSpec))
			t.Run(fmt.Sprintf("ActionNames() - %s", dsName), testActionNamesFunc(dsSpec))
			t.Run(fmt.Sprintf("Laws() - %s", dsName), testLawsFunc(dsSpec))
		}
	}
}

// Tests DataSource creation from DataSourceSpec
func testNewDataSourceFunc(dsSpec spec.DataSourceSpec) func(*testing.T) {
	return func(t *testing.T) {
		ds, err := NewDataSource(dsSpec)
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
func testActionsFunc(dsSpec spec.DataSourceSpec) func(*testing.T) {
	return func(t *testing.T) {
		ds, err := NewDataSource(dsSpec)
		if err != nil {
			t.Error(err)
		}

		actual := ds.Actions()

		var expected map[string]string

		switch ds.Name() {
		case "local/portfolio":
			expected = map[string]string{
				"local.portfolio.buy":  "local.portfolio.usd_balance -= args.price\nlocal.portfolio.btc_balance += 1",
				"local.portfolio.sell": "local.portfolio.usd_balance += args.price\nlocal.portfolio.btc_balance -= 1",
			}
		case "coinbase/btcusd":
			expected = make(map[string]string)

		case "gym/CartPole-v1":
			expected = map[string]string{
				"gym.CartPole-v1.left":  "passthru",
				"gym.CartPole-v1.right": "passthru",
			}
		}
		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("Expected:\n%v\nGot:\n%v", expected, actual)
		}
	}
}

// Tests Fields() getter
func testFieldsFunc(dsSpec spec.DataSourceSpec) func(*testing.T) {
	return func(t *testing.T) {
		ds, err := NewDataSource(dsSpec)
		if err != nil {
			t.Error(err)
		}

		actual := ds.Fields()

		var expected map[string]float64

		switch ds.Name() {
		case "local/portfolio":
			expected = map[string]float64{
				"local.portfolio.usd_balance": 1000000,
				"local.portfolio.btc_balance": 0,
			}
		case "coinbase/btcusd":
			expected = map[string]float64{
				"coinbase.btcusd.price": 0,
			}

		case "gym/CartPole-v1":
			expected = map[string]float64{
				"gym.CartPole-v1.pole_angle":            0,
				"gym.CartPole-v1.pole_angular_velocity": 0,
				"gym.CartPole-v1.position":              0,
				"gym.CartPole-v1.velocity":              0,
			}
		}

		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("Expected:\n%v\nGot:\n%v", expected, actual)
		}
	}
}

// Tests FieldNames() getter
func testFieldNamesFunc(dsSpec spec.DataSourceSpec) func(*testing.T) {
	return func(t *testing.T) {
		ds, err := NewDataSource(dsSpec)
		if err != nil {
			t.Error(err)
		}

		actual := ds.FieldNames()

		var expected map[string]string

		switch ds.Name() {
		case "local/portfolio":
			expected = map[string]string{
				"usd_balance": "local.portfolio.usd_balance",
				"btc_balance": "local.portfolio.btc_balance",
			}
		case "coinbase/btcusd":
			expected = map[string]string{
				"price": "coinbase.btcusd.price",
			}

		case "gym/CartPole-v1":
			expected = map[string]string{
				"pole_angle":            "gym.CartPole-v1.pole_angle",
				"pole_angular_velocity": "gym.CartPole-v1.pole_angular_velocity",
				"position":              "gym.CartPole-v1.position",
				"velocity":              "gym.CartPole-v1.velocity",
			}
		}

		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("Expected:\n%v\nGot:\n%v", expected, actual)
		}
	}
}

// Tests ActionNames() getter
func testActionNamesFunc(dsSpec spec.DataSourceSpec) func(*testing.T) {
	return func(t *testing.T) {
		ds, err := NewDataSource(dsSpec)
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
		case "coinbase/btcusd":
			expected = make(map[string]string)

		case "gym/CartPole-v1":
			expected = map[string]string{
				"left":  "gym.CartPole-v1.left",
				"right": "gym.CartPole-v1.right",
			}
		}

		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("Expected:\n%v\nGot:\n%v", expected, actual)
		}
	}
}

// Tests Laws() getter
func testLawsFunc(dsSpec spec.DataSourceSpec) func(*testing.T) {
	return func(t *testing.T) {
		ds, err := NewDataSource(dsSpec)
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
		case "gym/CartPole-v1":
			// No laws
		}

		if !reflect.DeepEqual(expected, actual) {
			t.Errorf("Expected:\n%v\nGot:\n%v", expected, actual)
		}
	}
}
