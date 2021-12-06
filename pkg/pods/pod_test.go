package pods

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/data-components-contrib/dataconnectors/file"
	"github.com/spiceai/spiceai/pkg/spec"
	"github.com/spiceai/spiceai/pkg/state"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/pods"))

func TestPod(t *testing.T) {
	type TestPodParams struct {
		LocalStateTest   bool
		ExpectedHash     string
		ValidForTraining bool
	}
	manifestsToTest := map[string]*TestPodParams{
		"trader.yaml": {
			LocalStateTest:   true,
			ExpectedHash:     "f5d076339a77c6036e1535f1647cf5ab",
			ValidForTraining: true,
		},
		"trader-infer.yaml": {
			LocalStateTest:   true,
			ExpectedHash:     "ea70b58d28538842a1cb52fdb7d5b11f",
			ValidForTraining: false,
		},
		"event-tags.yaml": {
			LocalStateTest:   false,
			ExpectedHash:     "1dea04f71a65220474eb690252700f99",
			ValidForTraining: true,
		},
		"event-tags-invalid.yaml": {
			LocalStateTest:   false,
			ExpectedHash:     "d0e9abb23337df113999fb51560165d2",
			ValidForTraining: false,
		},
		"event-categories.yaml": {
			LocalStateTest:   false,
			ExpectedHash:     "9879535af17608c087d519b74a497f35",
			ValidForTraining: true,
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
		t.Run(fmt.Sprintf("Tags() - %s", manifestToTest), testTagsFunc(pod))
		t.Run(fmt.Sprintf("CachedCsv() - %s", manifestToTest), testCachedCsvFunc(pod))
		t.Run(fmt.Sprintf("ValidateForTraining() - %s", manifestToTest), testValidateForTraining(pod, testParams.ValidForTraining))

		if testParams.LocalStateTest {
			t.Run(fmt.Sprintf("AddLocalState() - %s", manifestToTest), testAddLocalStateFunc(pod))
			t.Run(fmt.Sprintf("AddLocalState()/CachedCsv() - %s", manifestToTest), testAddLocalStateCachedCsvFunc(pod))
		}
	}
}

func testValidateForTraining(pod *Pod, validForTraining bool) func(*testing.T) {
	return func(t *testing.T) {
		if validForTraining {
			assert.NoError(t, pod.ValidateForTraining())
		} else {
			assert.Error(t, pod.ValidateForTraining())
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
		case "event-tags-invalid":
			expected = "../../test/assets/pods/manifests/event-tags-invalid.yaml"
		case "event-categories":
			expected = "../../test/assets/pods/manifests/event-categories.yaml"
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
			fallthrough
		case "event-tags-invalid":
			expected = "1610057400"
		case "event-categories":
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
			fallthrough
		case "event-tags-invalid":
			expected = "24h0m0s"
		case "event-categories":
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
			fallthrough
		case "event-tags-invalid":
			expected = "10m0s"
		case "event-categories":
			expected = "1h6m40s"
		}

		assert.Equal(t, expected, actual, "invalid pod.Interval()")

		actual = pod.Granularity().String()

		switch pod.Name {
		case "trader":
			expected = "17s"
		case "trader-infer":
			expected = "10s"
		case "event-tags":
			fallthrough
		case "event-tags-invalid":
			expected = "30s"
		case "event-categories":
			expected = "6m40s"
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
				"event.data.height",
				"event.data.rating",
				"event.data.speed",
				"event.data.target",
			}
		case "event-tags-invalid":
			expected = []string{
				"event.data-invalid.height",
				"event.data-invalid.rating",
				"event.data-invalid.speed",
				"event.data-invalid.target",
			}
		case "event-categories":
			expected = []string{
				"event.stream.duration",
				"event.stream.guest_count",
				"event.stream.ticket_price",
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
				"buy":  "new_price = next_state[\"coinbase_btcusd_close\"]\nchange_in_price = prev_price - new_price\nreward = change_in_price\n",
				"sell": "new_price = next_state[\"coinbase_btcusd_close\"]\nchange_in_price = prev_price - new_price\nreward = -change_in_price\n",
				"hold": "reward = 1",
			}
		case "trader-infer":
			expected = map[string]string{
				"buy":  "reward = 1",
				"sell": "reward = 1",
			}
		case "event-tags":
			fallthrough
		case "event-tags-invalid":
			expected = map[string]string{
				"action_one": "reward = 1",
				"action_two": "reward = 1",
			}
		case "event-categories":
			expected = map[string]string{
				"action_one": "reward = 1",
				"action_two": "reward = 1",
			}
		}

		assert.Equal(t, expected, actual, "invalid pod.Rewards()", pod.Name)
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
			fallthrough
		case "event-tags-invalid":
			expected = map[string]string{"action_one": "", "action_two": ""}
		case "event-categories":
			expected = map[string]string{"action_one": "", "action_two": ""}
		default:
			t.Errorf("invalid pod %s", pod.Name)
		}

		assert.Equal(t, expected, actual, "invalid pod.Actions()")
	}
}

// Tests Tags() getter
func testTagsFunc(pod *Pod) func(*testing.T) {
	return func(t *testing.T) {
		actualTags := pod.Tags()

		err := snapshotter.SnapshotMulti(pod.Name+"_tags", actualTags)
		if err != nil {
			t.Fatal(err)
		}
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

// Tests AddLocalState()
func testAddLocalStateFunc(pod *Pod) func(*testing.T) {
	return func(t *testing.T) {
		epoch := time.Unix(1605312000, 0)
		period := 7 * 24 * time.Hour
		interval := time.Hour

		fileConnector := file.NewFileConnector()

		var fileData []byte

		done := make(chan bool, 1)
		err := fileConnector.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
			fileData = data
			done <- true
			return nil, nil
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

		newState, err := state.GetStateFromCsv(nil, nil, nil, fileData)
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

		var wg sync.WaitGroup
		wg.Add(2)

		err := pod.InitDataConnectors(func(state *state.State, metadata map[string]string) error {
			wg.Done()
			return nil
		})
		if err != nil {
			t.Error(err)
		}

		fileConnector := file.NewFileConnector()

		var fileData []byte
		err = fileConnector.Read(func(data []byte, metadata map[string]string) ([]byte, error) {
			fileData = data
			wg.Done()
			return nil, nil
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

		measurements := []string{"local.portfolio.usd_balance", "local.portfolio.btc_balance", "coinbase.btcusd.price"}

		newState, err := state.GetStateFromCsv(nil, measurements, nil, fileData)
		if err != nil {
			t.Error(err)
		}

		assert.Equal(t, 1, len(newState), "expected one state object for coinbase")

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
