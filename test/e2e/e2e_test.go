package e2e

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/logrusorgru/aurora"
	"github.com/spiceai/spiceai/pkg/tempdir"
	"github.com/stretchr/testify/assert"
)

const (
	BaseUrl = "http://localhost:8000"
)

var (
	shouldRunTest      bool
	shouldStartRuntime bool
	spicedContext      string
	testDir            string
	repoRoot           string
	workingDirectory   string
	runtimePath        string
	cliClient          *cli
	runtime            *runtimeServer
	snapshotter        *cupaloy.Config
	testPods           = []string{"test/Trader@0.3.1", "test/customprocessor@0.1.0"}
)

func TestMain(m *testing.M) {
	flag.BoolVar(&shouldRunTest, "e2e", false, "run e2e tests")
	flag.BoolVar(&shouldStartRuntime, "startruntime", true, "start runtime")
	flag.StringVar(&spicedContext, "context", "docker", "specify --context <context> to spice CLI for spiced")
	flag.Parse()
	if !shouldRunTest {
		os.Exit(m.Run())
	}

	var err error
	testDir, err = tempdir.CreateTempDir("e2e")
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	workingDirectory, err = os.Getwd()
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	repoRoot = filepath.Join(workingDirectory, "../..")

	err = validateRepoRoot(repoRoot)
	if err != nil {
		log.Println(fmt.Errorf("not a valid repo root: %w", err).Error())
		os.Exit(1)
	}

	snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory(filepath.Join(repoRoot, "test/assets/snapshots/e2e")))

	cliPath := filepath.Join(repoRoot, "cmd/spice/spice")
	err = validateExists(cliPath)
	if err != nil {
		log.Println(fmt.Errorf("cli not built: %w", err))
		os.Exit(1)
	}

	runtimePath = filepath.Join(repoRoot, "cmd/spiced/spiced")
	err = validateExists(runtimePath)
	if err != nil {
		log.Println(fmt.Errorf("spiced runtime not built: %w", err))
		os.Exit(1)
	}

	_, err = os.Stat(testDir)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	cliClient = &cli{
		workingDir: testDir,
		cliPath:    cliPath,
	}

	runtime = &runtimeServer{
		baseUrl:            BaseUrl,
		shouldStartRuntime: shouldStartRuntime,
		runtimePath:        runtimePath,
		workingDirectory:   testDir,
		cli:                cliClient,
		context:            spicedContext,
	}

	for _, testPod := range testPods {
		err = cliClient.runCliCmd("add", testPod)
		if err != nil {
			log.Println(err.Error())
			os.Exit(1)
		}
	}

	testCode := m.Run()

	err = tempdir.RemoveAllCreatedTempDirectories()
	if err != nil {
		log.Println(err.Error())
	}

	os.Exit(testCode)
}

func TestPods(t *testing.T) {
	if !shouldRunTest {
		t.Skip("Specify '-e2e' to run e2e tests")
		return
	}

	err := runtime.startRuntime()
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		err := runtime.shutdown()
		if err != nil {
			log.Fatalln(err.Error())
		}
	})

	observation, err := runtime.getPods()
	if err != nil {
		t.Fatal(err)
	}

	var pods []map[string]string
	err = json.Unmarshal([]byte(observation), &pods)
	if err != nil {
		t.Fatal(err)
	}

	assert.Len(t, pods, 2)

	snapshotter.SnapshotT(t, pods[0]["name"], pods[1]["name"])
}

func TestObservations(t *testing.T) {
	if !shouldRunTest {
		t.Skip("Specify '-e2e' to run e2e tests")
		return
	}

	err := runtime.startRuntime()
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		err := runtime.shutdown()
		if err != nil {
			log.Fatalln(err.Error())
		}
	})

	observation, err := runtime.getObservations("trader")
	if err != nil {
		t.Fatal(err)
	}

	err = snapshotter.SnapshotMulti("initial_observation.csv", observation)
	if err != nil {
		t.Fatal(err)
	}

	newObservations, err := os.ReadFile(filepath.Join(repoRoot, "test/assets/data/csv/e2e_additional_observations.csv"))
	if err != nil {
		t.Fatal(err)
	}

	err = runtime.postObservations("trader", newObservations)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	observation, err = runtime.getObservations("trader")
	if err != nil {
		t.Fatal(err)
	}

	err = snapshotter.SnapshotMulti("new_observation.csv", observation)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDataspaceData(t *testing.T) {
	if !shouldRunTest {
		t.Skip("Specify '-e2e' to run e2e tests")
		return
	}

	err := runtime.startRuntime()
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		err := runtime.shutdown()
		if err != nil {
			log.Fatalln(err.Error())
		}
	})

	observation, err := runtime.getObservations("customprocessor")
	if err != nil {
		t.Fatal(err)
	}

	err = snapshotter.SnapshotMulti("initial_observation.csv", observation)
	if err != nil {
		t.Fatal(err)
	}

	newJsonData, err := os.ReadFile(filepath.Join(repoRoot, "test/assets/data/json/customprocessor.json"))
	if err != nil {
		t.Fatal(err)
	}

	err = runtime.postDataspace("customprocessor", "json", "processor", newJsonData)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	observation, err = runtime.getObservations("customprocessor")
	if err != nil {
		t.Fatal(err)
	}

	err = snapshotter.SnapshotMulti("new_observation.csv", observation)
	if err != nil {
		t.Fatal(err)
	}

	newCsvData, err := os.ReadFile(filepath.Join(repoRoot, "test/assets/data/csv/customprocessor.csv"))
	if err != nil {
		t.Fatal(err)
	}

	err = runtime.postDataspace("customprocessor", "csv", "processor", newCsvData)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	observation, err = runtime.getObservations("customprocessor")
	if err != nil {
		t.Fatal(err)
	}

	err = snapshotter.SnapshotMulti("new_observation_after_new_csv.csv", observation)
	if err != nil {
		t.Fatal(err)
	}
}

func TestInterpretations(t *testing.T) {
	if !shouldRunTest {
		t.Skip("Specify '-e2e' to run e2e tests")
		return
	}

	err := runtime.startRuntime()
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		err := runtime.shutdown()
		if err != nil {
			log.Fatalln(err.Error())
		}
	})

	var podEpochTime int64 = 1605312000

	interpretations, err := runtime.getInterpretations("trader", podEpochTime, podEpochTime)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 0, len(interpretations))

	newInterpretations, err := os.ReadFile(filepath.Join(repoRoot, "test/assets/data/json/e2e_additional_interpretations.json"))
	if err != nil {
		t.Fatal(err)
	}

	err = runtime.postInterpretations("trader", newInterpretations)
	if err != nil {
		t.Fatal(err)
	}

	interpretations, err = runtime.getInterpretations("trader", podEpochTime, podEpochTime)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(interpretations))

	err = snapshotter.Snapshot("interpretations.json", interpretations)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTrainingOutput(t *testing.T) {
	if !shouldRunTest {
		t.Skip("Specify '-e2e' to run e2e tests")
		return
	}

	err := runtime.startRuntime()
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		err := runtime.shutdown()
		if err != nil {
			log.Fatalln(err.Error())
		}
	})

	err = cliClient.runCliCmd("train", "trader", "--context", spicedContext)
	if err != nil {
		t.Fatal(err)
	}

	err = runtime.waitForTrainingToComplete("trader", "1" /*flight*/, 10)
	if err != nil {
		t.Fatal(err)
	}

	flights, err := runtime.getFlights("trader")
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, len(flights), 1, "expect 1 flight to be returned")
	flight := flights[0]
	assert.Equal(t, len(flight.Episodes), 10, "expect 10 episodes to be returned")
	for _, episode := range flight.Episodes {
		assert.Empty(t, episode.Error)
		assert.Empty(t, episode.ErrorMessage)

		var actionCount uint64
		var numActions int
		for _, count := range episode.ActionsTaken {
			actionCount += count
			numActions++
		}

		assert.Equal(t, 3, numActions, "expect 3 actions to be taken each episode")
		assert.Equal(t, uint64(1428), actionCount, "expect a total of 132 actions to be taken")
	}
}

func TestImportExport(t *testing.T) {
	if !shouldRunTest {
		t.Skip("Specify '-e2e' to run e2e tests")
		return
	}

	err := runtime.startRuntime()
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.shutdown() //nolint:errcheck

	err = cliClient.runCliCmd("train", "trader", "--context", spicedContext)
	if err != nil {
		t.Fatal(err)
	}

	newInterpretations, err := os.ReadFile(filepath.Join(repoRoot, "test/assets/data/json/e2e_additional_interpretations.json"))
	if err != nil {
		t.Fatal(err)
	}

	err = runtime.postInterpretations("trader", newInterpretations)
	if err != nil {
		t.Fatal(err)
	}

	err = runtime.waitForTrainingToComplete("trader", "1" /*flight*/, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("E2E: Training Completed!")
	time.Sleep(time.Second)

	err = cliClient.runCliCmd("export", "trader")
	if err != nil {
		t.Fatal(err)
	}

	_, err = os.Stat(filepath.Join(testDir, "trader.spicepod"))
	if err != nil {
		t.Fatal(fmt.Errorf("didn't see expected exported spicedpod: %w", err))
	}

	err = cliClient.runCliCmd("import", "trader.spicepod")
	if err != nil {
		t.Fatal(err)
	}

	inference, err := runtime.getRecommendation("trader", "latest")
	if err != nil {
		t.Fatal(err)
	}

	if inference.Confidence == 0.0 {
		t.Fatal(fmt.Errorf("expected the inference confidence to be greater than 0.0"))
	}

	t.Logf("%v\n", inference)

	var podEpochTime int64 = 1605312000
	interpretations, err := runtime.getInterpretations("trader", podEpochTime, podEpochTime)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(interpretations))

	err = snapshotter.Snapshot("interpretations.json", interpretations)
	if err != nil {
		t.Fatal(err)
	}

	// Now let's shutdown the runtime and restart it and import our exported pod
	err = runtime.shutdown()
	if err != nil {
		log.Fatalln(err.Error())
	}

	err = runtime.startRuntime()
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.shutdown() //nolint:errcheck

	err = cliClient.runCliCmd("import", "trader.spicepod")
	if err != nil {
		t.Fatal(err)
	}

	newInference, err := runtime.getRecommendation("trader", "latest")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; newInference.Action != inference.Action; i++ {
		newInference, err = runtime.getRecommendation("trader", "latest")
		if err != nil {
			t.Fatal(err)
		}

		fmt.Printf("%v\n", newInference)

		if i > 50 {
			t.Fatal(fmt.Errorf("didn't get a similar inference result after 50 tries"))
		}
	}

	fmt.Printf("%v\n", newInference)

	if newInference.Confidence != inference.Confidence {
		t.Fatal(fmt.Errorf("%s: the confidence values are different between the exported and imported models", aurora.Red("error")))
	}

	interpretations, err = runtime.getInterpretations("trader", podEpochTime, podEpochTime)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(interpretations))

	err = snapshotter.Snapshot("interpretations.json", interpretations)
	if err != nil {
		t.Fatal(err)
	}

	err = runtime.shutdown()
	if err != nil {
		t.Fatal(err)
	}
}

func validateRepoRoot(repoRoot string) error {
	return validateExists(filepath.Join(repoRoot, "go.mod"))
}

func validateExists(path string) error {
	_, err := os.Stat(path)
	return err
}
