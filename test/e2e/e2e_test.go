package e2e

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/logrusorgru/aurora"
	"github.com/spiceai/spice/pkg/tempdir"
	"github.com/stretchr/testify/assert"
)

const (
	BaseUrl = "http://localhost:8000"
)

var (
	shouldRunTest    *bool
	testDir          string
	repoRoot         string
	workingDirectory string
	runtimePath      string
	cliClient        *cli
	runtime          *runtimeServer
	snapshotter      *cupaloy.Config
)

func TestMain(m *testing.M) {
	shouldRunTest = flag.Bool("e2e", false, "run e2e tests")
	flag.Parse()
	if !*shouldRunTest {
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
		baseUrl: BaseUrl,
	}

	err = copyFile(filepath.Join(repoRoot, "test/assets/data/csv/COINBASE_BTCUSD, 30.csv"), testDir)
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	err = cliClient.runCliCmd("add", "test/Trader")
	if err != nil {
		log.Println(err.Error())
		os.Exit(1)
	}

	testCode := m.Run()

	err = tempdir.RemoveAllCreatedTempDirectories()
	if err != nil {
		log.Println(err.Error())
	}

	os.Exit(testCode)
}

func TestObservations(t *testing.T) {
	if !*shouldRunTest {
		t.Skip("Specify '-e2e' to run e2e tests")
		return
	}

	runtimeCmd, err := runtime.startRuntime(runtimePath, testDir)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		err = runtimeCmd.Process.Signal(os.Interrupt)
		if err != nil {
			t.Fatal(err)
		}
		err = runtimeCmd.Wait()
		if err != nil {
			t.Fatal(err)
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

	observation, err = runtime.getObservations("trader")
	if err != nil {
		t.Fatal(err)
	}

	err = snapshotter.SnapshotMulti("new_observation.csv", observation)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTrainingOutput(t *testing.T) {
	if !*shouldRunTest {
		t.Skip("Specify '-e2e' to run e2e tests")
		return
	}

	runtimeCmd, err := runtime.startRuntime(runtimePath, testDir)
	if err != nil {
		t.Fatal(err)
	}

	t.Cleanup(func() {
		err = runtimeCmd.Process.Signal(os.Interrupt)
		if err != nil {
			t.Fatal(err)
		}
		err = runtimeCmd.Wait()
		if err != nil {
			t.Fatal(err)
		}
	})

	err = cliClient.runCliCmd("train", "trader", "--context", "metal")
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
		assert.Equal(t, uint64(132), actionCount, "expect a total of 132 actions to be taken")
	}
}

func TestImportExport(t *testing.T) {
	if !*shouldRunTest {
		t.Skip("Specify '-e2e' to run e2e tests")
		return
	}

	runtimeCmd, err := runtime.startRuntime(runtimePath, testDir)
	if err != nil {
		t.Fatal(err)
	}
	defer runtimeCmd.Process.Kill() //nolint:errcheck

	err = cliClient.runCliCmd("train", "trader", "--context", "metal")
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

	inference, err := runtime.getInference("trader", "latest")
	if err != nil {
		t.Fatal(err)
	}

	if inference.Confidence == 0.0 {
		t.Fatal(fmt.Errorf("expected the inference confidence to be greater than 0.0"))
	}

	t.Logf("%v\n", inference)

	// Now let's shutdown the runtime and restart it and import our exported pod
	err = runtimeCmd.Process.Signal(os.Interrupt)
	if err != nil {
		t.Fatal(err)
	}
	err = runtimeCmd.Wait()
	if err != nil {
		t.Fatal(err)
	}

	runtimeCmd, err = runtime.startRuntime(runtimePath, testDir)
	if err != nil {
		t.Fatal(err)
	}
	defer runtimeCmd.Process.Kill() //nolint:errcheck

	err = cliClient.runCliCmd("import", "trader.spicepod")
	if err != nil {
		t.Fatal(err)
	}

	newInference, err := runtime.getInference("trader", "latest")
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; newInference.Action != inference.Action; i++ {
		newInference, err = runtime.getInference("trader", "latest")
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

	err = runtimeCmd.Process.Signal(os.Interrupt)
	if err != nil {
		t.Fatal(err)
	}
	err = runtimeCmd.Wait()
	if err != nil {
		t.Fatal(err)
	}
}

func copyFile(sourceFile string, targetPath string) error {
	fileContent, err := os.ReadFile(sourceFile)
	if err != nil {
		return err
	}

	stat, err := os.Stat(targetPath)
	if err != nil {
		return err
	}

	filename := filepath.Base(sourceFile)

	err = os.WriteFile(filepath.Join(targetPath, filename), fileContent, stat.Mode())
	if err != nil {
		return err
	}

	return nil
}

func validateRepoRoot(repoRoot string) error {
	return validateExists(filepath.Join(repoRoot, "go.mod"))
}

func validateExists(path string) error {
	_, err := os.Stat(path)
	return err
}
