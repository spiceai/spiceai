package pods

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/data-components-contrib/dataconnectors/file"
	"github.com/spiceai/data-components-contrib/dataprocessors"
	"github.com/spiceai/data-components-contrib/dataprocessors/csv"
	"github.com/spiceai/spiceai/pkg/spec"
	"github.com/spiceai/spiceai/pkg/state"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/pods"))

func TestPod(t *testing.T) {
	type TestPodParams struct {
		LocalStateTest bool
		ExpectedHash   string
	}
	manifestsToTest := map[string]*TestPodParams{
		"trader.yaml": {
			LocalStateTest: true,
			ExpectedHash:   "83ba69e37dd58ba0b607c6ebd88d9f96",
		},
		"trader-infer.yaml": {
			LocalStateTest: true,
			ExpectedHash:   "ea70b58d28538842a1cb52fdb7d5b11f",
		},
		"event-tags.yaml": {
			LocalStateTest: false,
			ExpectedHash:   "31083813b0495927b0d94a8b19b1889e",
		},
	}

	for manifestToTest, testParams := range manifestsToTest {
		manifestPath := filepath.Join("../../test/assets/pods/manifests", manifestToTest)

		pod, err := LoadPodFromManifest(manifestPath)
		if err != nil {
			t.Error(err)
			return
		}

		t.Run(fmt.Sprintf("Base Properties - %s", manifestToTest), testBasePropertiesFunc(pod, testParams.ExpectedHash))
		t.Run(fmt.Sprintf("MeasurementNames() - %s", manifestToTest), testMeasurementNamesFunc(pod))
		t.Run(fmt.Sprintf("Rewards() - %s", manifestToTest), testRewardsFunc(pod))
		t.Run(fmt.Sprintf("Actions() - %s", manifestToTest), testActionsFunc(pod))
		t.Run(fmt.Sprintf("CachedCsv() - %s", manifestToTest), testCachedCsvFunc(pod))
		t.Run(fmt.Sprintf("TagPathMap() - %s", manifestToTest), testTagPathMap(pod))

		if testParams.LocalStateTest {
			t.Run(fmt.Sprintf("AddLocalState() - %s", manifestToTest), testAddLocalStateFunc(pod))
			t.Run(fmt.Sprintf("AddLocalState()/CachedCsv() - %s", manifestToTest), testAddLocalStateCachedCsvFunc(pod))
		}
	}
}

// Tests base properties
func testBasePropertiesFunc(pod *Pod, expectedHash string) func(*testing.T) {
	return func(t *testing.T) {

		actual := pod.Hash()

		var expected string

		assert.Equal(t, expectedHash, actual, "invalid pod.Hash()")

		actual = pod.ManifestPath()

		switch pod.Name {
		case "trader":
			expected = "../../test/assets/pods/manifests/trader.yaml"
		case "trader-infer":
			expected = "../../test/assets/pods/manifests/trader-infer.yaml"
		case "event-tags":
			expected = "../../test/assets/pods/manifests/event-tags.yaml"
		}

		assert.Equal(t, expected, actual, "invalid pod.ManifestPath()")

		actual = fmt.Sprintf("%d", pod.Epoch().Unix())

		switch pod.Name {
		case "trader":
			expected = "1605312000"
		case "trader-infer":
			actual = actual[:8] // Reduce precision to test
			expected = fmt.Sprintf("%d", time.Now().Add(-pod.Period()).Unix())[:8]
		case "event-tags":
			expected = "1610057400"
		}

		assert.Equal(t, expected, actual, "invalid pod.Epoch()")

		actual = pod.Period().String()

		switch pod.Name {
		case "trader":
			expected = "17h0m0s"
		case "trader-infer":
			expected = "72h0m0s"
		case "event-tags":
			expected = "24h0m0s"
		}

		assert.Equal(t, expected, actual, "invalid pod.Period()")

		actual = pod.Interval().String()

		switch pod.Name {
		case "trader":
			expected = "17m0s"
		case "trader-infer":
			expected = "1m0s"
		case "event-tags":
			expected = "10m0s"
		}

		assert.Equal(t, expected, actual, "invalid pod.Interval()")

		actual = pod.Granularity().String()

		switch pod.Name {
		case "trader":
			expected = "17s"
		case "trader-infer":
			expected = "10s"
		case "event-tags":
			expected = "30s"
		}

		assert.Equal(t, expected, actual, "invalid pod.Granularity()")
	}
}

// Tests MeasurementNames() getter
func testMeasurementNamesFunc(pod *Pod) func(*testing.T) {
	return func(t *testing.T) {
		actual := pod.MeasurementNames()

		var expected []string

		switch pod.Name {
		case "trader":
			fallthrough
		case "trader-infer":
			expected = []string{
				"coinbase.btcusd.close",
				"local.portfolio.btc_balance",
				"local.portfolio.usd_balance",
			}
		case "event-tags":
			expected = []string{
				"event.data.eventId",
				"event.data.height",
				"event.data.rating",
				"event.data.speed",
				"event.data.target",
			}
		}

		assert.Equal(t, expected, actual, "invalid pod.MeasurementNames()")
	}
}

// Tests Rewards() getter
func testRewardsFunc(pod *Pod) func(*testing.T) {
	return func(t *testing.T) {
		actual := pod.Rewards()

		var expected map[string]string

		switch pod.Name {
		case "trader":
			expected = map[string]string{
				"buy":  "new_price = new_state.coinbase.btcusd.close\nchange_in_price = prev_price - new_price\nreward = change_in_price\n",
				"sell": "new_price = new_state.coinbase.btcusd.close\nchange_in_price = prev_price - new_price\nreward = -change_in_price\n",
				"hold": "reward = 1",
			}
		case "trader-infer":
			expected = map[string]string{
				"buy":  "reward = 1",
				"sell": "reward = 1",
			}
		case "event-tags":
			expected = map[string]string{
				"action_one": "reward = 1",
				"action_two": "reward = 1",
			}
		}

		assert.Equal(t, expected, actual, "invalid pod.Rewards()")
	}
}

// Tests Actions() getter
func testActionsFunc(pod *Pod) func(*testing.T) {
	return func(t *testing.T) {
		actual := pod.Actions()

		var expected map[string]string

		switch pod.Name {
		case "trader":
			expected = map[string]string{
				"buy":  "local.portfolio.usd_balance -= coinbase.btcusd.close\nlocal.portfolio.btc_balance += 1.1",
				"hold": "",
				"sell": "local.portfolio.usd_balance += coinbase.btcusd.close\nlocal.portfolio.btc_balance -= 1",
			}
		case "trader-infer":
			expected = map[string]string{
				"buy":  "local.portfolio.usd_balance -= args.price\nlocal.portfolio.btc_balance += 1",
				"sell": "local.portfolio.usd_balance += args.price\nlocal.portfolio.btc_balance -= 1",
			}
		case "event-tags":
			expected = map[string]string{"action_one": "", "action_two": ""}
		default:
			t.Errorf("invalid pod %s", pod.Name)
		}

		assert.Equal(t, expected, actual, "invalid pod.Actions()")
	}
}

// Tests CachedCsv() getter
func testCachedCsvFunc(pod *Pod) func(*testing.T) {
	return func(t *testing.T) {
		done := make(chan bool, 1)

		err := pod.InitDataConnectors(func(state *state.State, metadata map[string]string) error {
			done <- true
			return nil
		})
		if err != nil {
			t.Fatal(err.Error())
		}

		<-done

		actual := pod.CachedCsv()

		snapshotter.SnapshotT(t, actual)
	}
}

// Tests TagPathMap()
func testTagPathMap(pod *Pod) func(*testing.T) {
	return func(t *testing.T) {
		actualMap := pod.TagPathMap()

		var expected map[string][]string
		switch pod.Name {
		case "event-tags":
			expected = map[string][]string{"event.data": {"tagA", "tagB", "tagC"}}
		default:
			expected = make(map[string][]string)
		}

		assert.Equal(t, expected, actualMap)
	}
}

// Tests AddLocalState()
func testAddLocalStateFunc(pod *Pod) func(*testing.T) {
	return func(t *testing.T) {
		epoch := time.Unix(1605312000, 0)
		period := 7 * 24 * time.Hour
		interval := time.Hour

		dp, err := dataprocessors.NewDataProcessor(csv.CsvProcessorName)
		if err != nil {
			t.Error(err)
		}

		err = dp.Init(nil)
		assert.NoError(t, err)

		fileConnector := file.NewFileConnector()

		done := make(chan bool, 1)
		err = fileConnector.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
			_, err := dp.OnData(data)
			assert.NoError(t, err)
			done <- true
			return nil, err
		})
		if err != nil {
			t.Fatal(err.Error())
		}

		err = fileConnector.Init(epoch, period, interval, map[string]string{
			"path":  "../../test/assets/data/csv/trader_input.csv",
			"watch": "false",
		})
		if err != nil {
			t.Fatal(err.Error())
		}

		<-done

		newState, err := dp.GetState(nil)
		if err != nil {
			t.Error(err)
		}

		pod.AddLocalState(newState...)
	}
}

// Tests CachedCsv() called after AddLocalState()
func testAddLocalStateCachedCsvFunc(pod *Pod) func(*testing.T) {
	return func(t *testing.T) {
		epoch := time.Unix(1605312000, 0)
		period := 7 * 24 * time.Hour
		interval := time.Hour

		dp, err := dataprocessors.NewDataProcessor(csv.CsvProcessorName)
		if err != nil {
			t.Error(err)
		}

		err = dp.Init(nil)
		assert.NoError(t, err)

		var wg sync.WaitGroup
		wg.Add(2)

		err = pod.InitDataConnectors(func(state *state.State, metadata map[string]string) error {
			wg.Done()
			return nil
		})
		if err != nil {
			t.Error(err)
		}

		fileConnector := file.NewFileConnector()

		err = fileConnector.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
			_, err := dp.OnData(data)
			assert.NoError(t, err)
			wg.Done()
			return nil, err
		})
		if err != nil {
			t.Fatal(err.Error())
		}

		err = fileConnector.Init(epoch, period, interval, map[string]string{
			"path":  "../../test/assets/data/csv/trader_input.csv",
			"watch": "false",
		})
		if err != nil {
			t.Fatal(err.Error())
		}

		wg.Wait()

		newState, err := dp.GetState(nil)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, 2, len(newState), "expected two state objects, one for local and one for coinbase")

		pod.AddLocalState(newState...)

		actual := pod.CachedCsv()

		snapshotter.SnapshotT(t, actual)
	}
}

// Tests loadParams()
func TestLoadParams(t *testing.T) {
	t.Run("loadParams() - defaults", testLoadParamsDefaultsFunc())
	t.Run("loadParams()", testLoadParamsFunc())
}

func testLoadParamsDefaultsFunc() func(*testing.T) {
	return func(t *testing.T) {
		pod := &Pod{
			PodSpec: spec.PodSpec{},
		}
		err := pod.loadParams()
		assert.NoError(t, err)

		assert.Equal(t, time.Now().Add(-pod.Period()).Unix()/10, pod.Epoch().Unix()/10)
		assert.Equal(t, 72*time.Hour, pod.Period())
		assert.Equal(t, 1*time.Minute, pod.Interval())
		assert.Equal(t, 10*time.Second, pod.Granularity())
	}
}

func testLoadParamsFunc() func(*testing.T) {
	return func(t *testing.T) {
		pod := &Pod{
			PodSpec: spec.PodSpec{
				Params: map[string]string{
					"epoch_time":  "123456789",
					"period":      "152h",
					"interval":    "355m",
					"granularity": "124s",
				},
			},
		}
		err := pod.loadParams()
		assert.NoError(t, err)

		assert.Equal(t, int64(123456789), pod.Epoch().Unix())
		assert.Equal(t, 152*time.Hour, pod.Period())
		assert.Equal(t, 355*time.Minute, pod.Interval())
		assert.Equal(t, 124*time.Second, pod.Granularity())
	}
}
