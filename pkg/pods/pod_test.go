package pods

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/data-components-contrib/dataconnectors/file"
	"github.com/spiceai/data-components-contrib/dataprocessors"
	"github.com/spiceai/data-components-contrib/dataprocessors/csv"
	"github.com/spiceai/spiceai/pkg/interpretations"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/pods"))

func TestPod(t *testing.T) {
	manifestsToTest := []string{"trader.yaml", "trader-infer.yaml"}

	for _, manifestToTest := range manifestsToTest {
		manifestPath := filepath.Join("../../test/assets/pods/manifests", manifestToTest)

		pod, err := LoadPodFromManifest(manifestPath)
		if err != nil {
			t.Error(err)
			return
		}

		t.Run(fmt.Sprintf("Base Properties - %s", manifestToTest), testBasePropertiesFunc(pod))
		t.Run(fmt.Sprintf("FieldNames() - %s", manifestToTest), testFieldNamesFunc(pod))
		t.Run(fmt.Sprintf("Rewards() - %s", manifestToTest), testRewardsFunc(pod))
		t.Run(fmt.Sprintf("Actions() - %s", manifestToTest), testActionsFunc(pod))
		t.Run(fmt.Sprintf("CachedCsv() - %s", manifestToTest), testCachedCsvFunc(pod))
		t.Run(fmt.Sprintf("AddLocalState() - %s", manifestToTest), testAddLocalStateFunc(pod))
		t.Run(fmt.Sprintf("AddLocalState()/CachedCsv() - %s", manifestToTest), testAddLocalStateCachedCsvFunc(pod))
		t.Run(fmt.Sprintf("GetInterpretations() - %s", manifestToTest), testGetInterpretationsFunc(pod))
	}
}

// Tests base properties
func testBasePropertiesFunc(pod *Pod) func(*testing.T) {
	return func(t *testing.T) {

		actual := pod.Hash()

		var expected string

		switch pod.Name {
		case "trader":
			expected = "64a15d213ebe84486fc68e209ca5d160"
		case "trader-infer":
			expected = "ae441bfd1c725a0f8dfbce525146d441"
		}

		assert.Equal(t, expected, actual, "invalid pod.Hash()")

		actual = pod.ManifestPath()

		switch pod.Name {
		case "trader":
			expected = "../../test/assets/pods/manifests/trader.yaml"
		case "trader-infer":
			expected = "../../test/assets/pods/manifests/trader-infer.yaml"
		}

		assert.Equal(t, expected, actual, "invalid pod.ManifestPath()")

		actual = fmt.Sprintf("%d", pod.Epoch().Unix())

		switch pod.Name {
		case "trader":
			expected = "1605312000"
		case "trader-infer":
			actual = actual[:8] // Reduce precision to test
			expected = fmt.Sprintf("%d", time.Now().Add(-pod.Period()).Unix())[:8]
		}

		assert.Equal(t, expected, actual, "invalid pod.Epoch()")

		actual = pod.Period().String()

		switch pod.Name {
		case "trader":
			expected = "17h0m0s"
		case "trader-infer":
			expected = "72h0m0s"
		}

		assert.Equal(t, expected, actual, "invalid pod.Period()")

		actual = pod.Interval().String()

		switch pod.Name {
		case "trader":
			expected = "17m0s"
		case "trader-infer":
			expected = "1m0s"
		}

		assert.Equal(t, expected, actual, "invalid pod.Interval()")

		actual = pod.Granularity().String()

		switch pod.Name {
		case "trader":
			expected = "17s"
		case "trader-infer":
			expected = "10s"
		}

		assert.Equal(t, expected, actual, "invalid pod.Granularity()")
	}
}

// Tests FieldNames() getter
func testFieldNamesFunc(pod *Pod) func(*testing.T) {
	return func(t *testing.T) {
		actual := pod.FieldNames()

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
		}

		assert.Equal(t, expected, actual, "invalid pod.FieldNames()")
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
		default:
			t.Errorf("invalid pod %s", pod.Name)
		}

		assert.Equal(t, expected, actual, "invalid pod.Actions()")
	}
}

// Tests CachedCsv() getter
func testCachedCsvFunc(pod *Pod) func(*testing.T) {
	return func(t *testing.T) {
		_, err := pod.FetchNewData()
		if err != nil {
			t.Error(err)
			return
		}

		actual := pod.CachedCsv()

		snapshotter.SnapshotT(t, actual)
	}
}

// Tests AddLocalState()
func testAddLocalStateFunc(pod *Pod) func(*testing.T) {
	return func(t *testing.T) {
		fileConnector := file.NewFileConnector()
		err := fileConnector.Init(map[string]string{
			"path":  "../../test/assets/data/csv/trader_input.csv",
			"watch": "false",
		})
		if err != nil {
			t.Fatal(err.Error())
		}
		data, err := fileConnector.FetchData(time.Unix(1605312000, 0), 7*24*time.Hour, time.Hour)
		if err != nil {
			t.Fatal(err.Error())
		}

		dp, err := dataprocessors.NewDataProcessor(csv.CsvProcessorName)
		if err != nil {
			t.Error(err)
		}

		err = dp.Init(nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

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
		fileConnector := file.NewFileConnector()
		err := fileConnector.Init(map[string]string{
			"path":  "../../test/assets/data/csv/trader_input.csv",
			"watch": "false",
		})
		if err != nil {
			t.Fatal(err.Error())
		}
		data, err := fileConnector.FetchData(time.Unix(1605312000, 0), 7*24*time.Hour, time.Hour)
		if err != nil {
			t.Fatal(err.Error())
		}

		dp, err := dataprocessors.NewDataProcessor(csv.CsvProcessorName)
		if err != nil {
			t.Error(err)
		}

		err = dp.Init(nil)
		assert.NoError(t, err)

		_, err = dp.OnData(data)
		assert.NoError(t, err)

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

// Tests GetIntepretations()
func testGetInterpretationsFunc(pod *Pod) func(*testing.T) {
	return func(t *testing.T) {
		startRange := pod.Epoch().Add(100 * time.Second)
		endRange := pod.Epoch().Add(200 * time.Second)

		var allInterpretations []interpretations.Interpretation
		var inRangeInterpretations []interpretations.Interpretation

		beforeRangeInterpretation, err := interpretations.NewInterpretation(startRange.Add(-2*time.Second), startRange.Add(-1*time.Second), "before range")
		if err != nil {
			t.Error(err)
		}

		err = pod.AddInterpretation(beforeRangeInterpretation)
		assert.NoError(t, err)
		allInterpretations = append(allInterpretations, *beforeRangeInterpretation)

		endOnRangeInterpretation, err := interpretations.NewInterpretation(startRange.Add(-1*time.Second), startRange, "end on range")
		if err != nil {
			t.Error(err)
		}

		err = pod.AddInterpretation(endOnRangeInterpretation)
		assert.NoError(t, err)
		allInterpretations = append(allInterpretations, *endOnRangeInterpretation)
		inRangeInterpretations = append(inRangeInterpretations, *endOnRangeInterpretation)

		withinRangeInterpretation, err := interpretations.NewInterpretation(startRange.Add(1*time.Second), startRange.Add(2*time.Second), "within range")
		if err != nil {
			t.Error(err)
		}

		err = pod.AddInterpretation(withinRangeInterpretation)
		assert.NoError(t, err)
		allInterpretations = append(allInterpretations, *withinRangeInterpretation)
		inRangeInterpretations = append(inRangeInterpretations, *withinRangeInterpretation)

		aroundRangeInterpretation, err := interpretations.NewInterpretation(startRange.Add(-1*time.Second), endRange.Add(1*time.Second), "around range")
		if err != nil {
			t.Error(err)
		}

		err = pod.AddInterpretation(aroundRangeInterpretation)
		assert.NoError(t, err)
		allInterpretations = append(allInterpretations, *aroundRangeInterpretation)
		inRangeInterpretations = append(inRangeInterpretations, *aroundRangeInterpretation)

		startOnRangeInterpretation, err := interpretations.NewInterpretation(endRange, endRange.Add(1*time.Second), "start on range")
		if err != nil {
			t.Error(err)
		}

		err = pod.AddInterpretation(startOnRangeInterpretation)
		assert.NoError(t, err)
		allInterpretations = append(allInterpretations, *startOnRangeInterpretation)
		inRangeInterpretations = append(inRangeInterpretations, *startOnRangeInterpretation)

		startAfterRangeInterpretation, err := interpretations.NewInterpretation(endRange.Add(1*time.Second), endRange.Add(2*time.Second), "start after range")
		if err != nil {
			t.Error(err)
		}

		err = pod.AddInterpretation(startAfterRangeInterpretation)
		assert.NoError(t, err)
		allInterpretations = append(allInterpretations, *startAfterRangeInterpretation)

		assert.Equal(t, 6, len(allInterpretations))
		assert.Equal(t, allInterpretations, pod.Interpretations())

		assert.Equal(t, 4, len(pod.GetInterpretations(startRange, endRange)))
		assert.Equal(t, inRangeInterpretations, pod.GetInterpretations(startRange, endRange))
	}
}
