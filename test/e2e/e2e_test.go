package e2e

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
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
	learningAlgorithm  string
	testDir            string
	repoRoot           string
	localRegistryPath  string
	workingDirectory   string
	runtimePath        string
	cliClient          *cli
	runtime            *runtimeServer
	snapshotter        *cupaloy.Config
	testPods           = []string{"test/Trader@0.4.0", "test/customprocessor@0.2.0", "test/event-tags@0.3.0", "test/event-categories@0.2.0", "test/trader-seed-streaming@0.1.0"}
)

func TestMain(m *testing.M) {
	flag.BoolVar(&shouldRunTest, "e2e", false, "run e2e tests")
	flag.BoolVar(&shouldStartRuntime, "startruntime", true, "start runtime")
	flag.StringVar(&spicedContext, "context", "docker", "specify --context <context> to spice CLI for spiced")
	flag.StringVar(&learningAlgorithm, "learning-algorithm", "dql", "specify --learning-alogrithm use for training")
	flag.StringVar(&localRegistryPath, "localregistry", "", "-localregistry <path> uses local Spicepod registry at <path> instead of spicerack.org")
	flag.Parse()
	if !shouldRunTest {
		os.Exit(m.Run())
	}

	fmt.Printf("Using %s context\n", aurora.BrightCyan(spicedContext))

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

	podsToAdd := testPods
	if localRegistryPath != "" {
		fmt.Printf("Adding pods from local registry %s\n", aurora.BrightBlue(localRegistryPath))
		podsToAdd = make([]string, len(testPods))
		for _, p := range testPods {
			pPath := strings.Split(p, "@")
			podsToAdd = append(podsToAdd, filepath.Join(localRegistryPath, "pods", pPath[0]))
		}
	}

	for _, testPod := range podsToAdd {
		if testPod == "" {
			continue
		}
		fmt.Printf("Running: spice add %s\n", testPod)
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

	t.Log("*** Get Pods ***")
	pods, err := runtime.getPods()
	if err != nil {
		t.Fatal(err)
	}

	assert.Len(t, pods, 5)

	sort.SliceStable(pods, func(i, j int) bool {
		return strings.Compare(pods[i]["name"].(string), pods[j]["name"].(string)) == -1
	})

	for _, pod := range pods {
		pod["manifest_path"] = "fixed"
	}

	podsData, err := json.MarshalIndent(pods, "", "  ")
	if err != nil {
		t.Fatal(err)
	}

	snapshotter.SnapshotT(t, podsData)
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

	t.Log("*** Get Observations ***")
	initialObservationsCsv, err := runtime.getObservations("trader", "")
	if err != nil {
		t.Fatal(err)
	}

	err = snapshotter.SnapshotMulti("initial_observation.csv", initialObservationsCsv)
	if err != nil {
		t.Fatal(err)
	}

	initialObservationsJson, err := runtime.getObservations("trader", "application/json")
	if err != nil {
		t.Fatal(err)
	}

	err = snapshotter.SnapshotMulti("initial_observation.json", initialObservationsJson)
	if err != nil {
		t.Fatal(err)
	}

	newObservations, err := os.ReadFile(filepath.Join(repoRoot, "test/assets/data/csv/e2e_additional_observations.csv"))
	if err != nil {
		t.Fatal(err)
	}

	t.Log("*** Post Observations ***")
	err = runtime.postObservations("trader", newObservations)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	t.Log("*** Get New Observations ***")
	newObservationsCsv, err := runtime.getObservations("trader", "")
	if err != nil {
		t.Fatal(err)
	}

	err = snapshotter.SnapshotMulti("new_observation.csv", newObservationsCsv)
	if err != nil {
		t.Fatal(err)
	}

	newObservationsJson, err := runtime.getObservations("trader", "application/json")
	if err != nil {
		t.Fatal(err)
	}

	err = snapshotter.SnapshotMulti("new_observation.json", newObservationsJson)
	if err != nil {
		t.Fatal(err)
	}
}

func TestSeedAndStreamingObservations(t *testing.T) {
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

	t.Log("*** Get Observations ***")
	initialObservationsCsv, err := runtime.getObservations("trader-seed-streaming", "")
	if err != nil {
		t.Fatal(err)
	}

	assert.Greater(t, len(initialObservationsCsv), 952)

	// Fetch the first 951 rows, which is the seed data
	seedObservations := strings.Join(strings.SplitN(initialObservationsCsv, "\n", 952)[:951], "\n")

	snapshotter.SnapshotT(t, seedObservations)

	// Check we have additional streaming prices
	additionalObservationsCsv, err := runtime.getObservations("trader-seed-streaming", "")
	if err != nil {
		t.Fatal(err)
	}

	// Fetch the first 951 rows, which is the seed data
	additionalObservations := strings.Split(additionalObservationsCsv, "\n")
	assert.Greater(t, len(additionalObservations), 5)
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

	pods, err := runtime.getPods()
	if err != nil {
		t.Fatal(err)
	}

	for _, pod := range pods {
		podName := pod["name"].(string)
		t.Logf("*** Get Observations for pod %s ***", podName)

		observations, err := runtime.getObservations(podName, "")
		if err != nil {
			t.Fatal(err)
		}

		if podName == "trader-seed-streaming" {
			observations = strings.Join(strings.SplitN(observations, "\n", 952)[:951], "\n")
		}

		err = snapshotter.SnapshotMulti(podName + "_initial_observations.csv", observations)
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestDataspaceDataUpdate(t *testing.T) {
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

	t.Log("*** Get Observations ***")
	observations, err := runtime.getObservations("customprocessor", "")
	if err != nil {
		t.Fatal(err)
	}

	err = snapshotter.SnapshotMulti("initial_observations.csv", observations)
	if err != nil {
		t.Fatal(err)
	}

	newJsonData, err := os.ReadFile(filepath.Join(repoRoot, "test/assets/data/json/customprocessor.json"))
	if err != nil {
		t.Fatal(err)
	}

	t.Log("*** Post Dataspace Json Data ***")
	err = runtime.postDataspace("customprocessor", "json", "processor", newJsonData)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	t.Log("*** Get New Observations ***")
	observations, err = runtime.getObservations("customprocessor", "")
	if err != nil {
		t.Fatal(err)
	}

	err = snapshotter.SnapshotMulti("new_observation.csv", observations)
	if err != nil {
		t.Fatal(err)
	}

	newCsvData, err := os.ReadFile(filepath.Join(repoRoot, "test/assets/data/csv/customprocessor.csv"))
	if err != nil {
		t.Fatal(err)
	}

	t.Log("*** Post Dataspace CSV Data ***")
	err = runtime.postDataspace("customprocessor", "csv", "processor", newCsvData)
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Second)

	t.Log("*** Get New Observations with CSV Data ***")
	observations, err = runtime.getObservations("customprocessor", "")
	if err != nil {
		t.Fatal(err)
	}

	err = snapshotter.SnapshotMulti("new_observation_after_new_csv.csv", observations)
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

	t.Log("*** Get Interpretation ***")
	interpretations, err := runtime.getInterpretations("trader", podEpochTime, podEpochTime)
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 0, len(interpretations))

	newInterpretations, err := os.ReadFile(filepath.Join(repoRoot, "test/assets/data/json/e2e_additional_interpretations.json"))
	if err != nil {
		t.Fatal(err)
	}

	t.Log("*** Post Interpretations ***")
	err = runtime.postInterpretations("trader", newInterpretations)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("*** Get New Interpretations ***")
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

	t.Log("*** Start Training ***")
	err = cliClient.runCliCmd("train", "trader", "--context", spicedContext, "--learning-algorithm", learningAlgorithm)
	if err != nil {
		t.Fatal(err)
	}

	err = runtime.waitForTrainingToComplete("trader", "1" /*flight*/, 10)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("*** Get Flights ***")
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
		assert.Equal(t, uint64(1428), actionCount, "unexpected actions taken")
	}
}

func TestPodWithTags(t *testing.T) {
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

	err = cliClient.runCliCmd("train", "event-tags", "--context", spicedContext, "--learning-algorithm", learningAlgorithm)
	if err != nil {
		t.Fatal(err)
	}

	err = runtime.waitForTrainingToComplete("event-tags", "1" /*flight*/, 4)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("*** Get Flights ***")
	flights, err := runtime.getFlights("event-tags")
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(flights), "expect 1 flight to be returned")
	flight := flights[0]
	assert.Equal(t, len(flight.Episodes), 4, "expect 4 episodes to be returned")
	for _, episode := range flight.Episodes {
		assert.Empty(t, episode.Error)
		assert.Empty(t, episode.ErrorMessage)

		var actionCount uint64
		var numActions int
		for _, count := range episode.ActionsTaken {
			actionCount += count
			numActions++
		}

		assert.Equal(t, 2, numActions, "expect 2 actions to be taken each episode")
		assert.Equal(t, uint64(33), actionCount, "unexpected actions taken")
	}
}

func TestPodWithCategories(t *testing.T) {
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

	err = cliClient.runCliCmd("train", "event-categories", "--context", spicedContext, "--learning-algorithm", learningAlgorithm)
	if err != nil {
		t.Fatal(err)
	}

	err = runtime.waitForTrainingToComplete("event-categories", "1" /*flight*/, 4)
	if err != nil {
		t.Fatal(err)
	}

	t.Log("*** Get Flights ***")
	flights, err := runtime.getFlights("event-categories")
	if err != nil {
		t.Fatal(err)
	}

	assert.Equal(t, 1, len(flights), "expect 1 flight to be returned")
	flight := flights[0]
	assert.Equal(t, len(flight.Episodes), 4, "expect 4 episodes to be returned")
	for _, episode := range flight.Episodes {
		assert.Empty(t, episode.Error)
		assert.Empty(t, episode.ErrorMessage)

		var actionCount uint64
		var numActions int
		for _, count := range episode.ActionsTaken {
			actionCount += count
			numActions++
		}

		assert.Equal(t, 2, numActions, "expect 2 actions to be taken each episode")
		assert.Equal(t, uint64(260), actionCount, "unexpected actions taken")
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

	err = cliClient.runCliCmd("train", "trader", "--context", spicedContext, "--learning-algorithm", learningAlgorithm)
	if err != nil {
		t.Fatal(err)
	}

	newInterpretations, err := os.ReadFile(filepath.Join(repoRoot, "test/assets/data/json/e2e_additional_interpretations.json"))
	if err != nil {
		t.Fatal(err)
	}

	t.Log("*** Post Interpretations ***")
	err = runtime.postInterpretations("trader", newInterpretations)
	if err != nil {
		t.Fatal(err)
	}

	err = runtime.waitForTrainingToComplete("trader", "1" /*flight*/, 10)
	if err != nil {
		t.Fatal(err)
	}
	t.Log("*** Training Completed ***")
	time.Sleep(time.Second)

	t.Log("*** Export Pod ***")
	err = cliClient.runCliCmd("export", "trader")
	if err != nil {
		t.Fatal(err)
	}

	_, err = os.Stat(filepath.Join(testDir, "trader.spicepod"))
	if err != nil {
		t.Fatal(fmt.Errorf("didn't see expected exported spicepod: %w", err))
	}

	t.Log("*** Import Pod ***")
	err = cliClient.runCliCmd("import", "trader.spicepod")
	if err != nil {
		t.Fatal(err)
	}

	t.Log("*** Get Recommendation ***")
	inference, err := runtime.getRecommendation("trader", "latest")
	if err != nil {
		t.Fatal(err)
	}

	if inference.Confidence == 0.0 {
		t.Fatal(fmt.Errorf("expected the inference confidence to be greater than 0.0"))
	}

	t.Logf("%v\n", inference)

	t.Log("*** Get Interpretations ***")
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

	t.Log("*** Shutdown Runtime ***")
	// Now let's shutdown the runtime and restart it and import our exported pod
	err = runtime.shutdown()
	if err != nil {
		log.Fatalln(err.Error())
	}

	t.Log("*** Start Runtime ***")
	err = runtime.startRuntime()
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.shutdown() //nolint:errcheck

	t.Log("*** Import Pod again ***")
	err = cliClient.runCliCmd("import", "trader.spicepod")
	if err != nil {
		t.Fatal(err)
	}

	t.Log("*** Get Recommendation again ***")
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

	t.Log("*** Get Interpretations again ***")
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

func TestCLICmdPodsList(t *testing.T) {
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

	output, err := cliClient.runCliCmdOutput("pods", "list")
	if err != nil {
		t.Fatal(err)
	}
	outputStr := string(output)
	assert.Contains(t, outputStr, "NAME")
	assert.Contains(t, outputStr, "MANIFEST PATH")

	// tablewriter adds padding to make columns uniform length so the header differs in
	// padding length across metal/docker context as we are snapshotting after replacing
	// 'testDir' and `testDir' is different across contexts
	lines := strings.Split(outputStr, "\n")
	partialOutput := strings.Join(lines[1:], "\n")

	fixedOutput := strings.ReplaceAll(partialOutput, "/private", "")
	fixedOutput = strings.ReplaceAll(fixedOutput, testDir, "/userapp")
	snapshotter.SnapshotT(t, fixedOutput)
}

func validateRepoRoot(repoRoot string) error {
	return validateExists(filepath.Join(repoRoot, "go.mod"))
}

func validateExists(path string) error {
	_, err := os.Stat(path)
	return err
}
