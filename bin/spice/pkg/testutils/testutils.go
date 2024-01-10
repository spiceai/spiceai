package testutils

import (
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/spiceai/spiceai/bin/spice/pkg/constants"
)

// This test suite is using a neat trick for testing exec.Command that is documented here:
// https://npf.io/2015/06/testing-exec-command/

func GetScenarioExecCommand(scenario string) func(command string, args ...string) *exec.Cmd {
	return func(command string, args ...string) *exec.Cmd {
		testCmdArgs := []string{"-test.run=TestAIEngineMockCmd", "--", command}
		testCmdArgs = append(testCmdArgs, args...)
		testCmd := exec.Command(os.Args[0], testCmdArgs...)
		testCmd.Env = []string{"GO_WANT_HELPER_PROCESS=1"}
		if scenario != "" {
			testCmd.Env = append(testCmd.Env, fmt.Sprintf("SCENARIO=%s", scenario))
		}
		return testCmd
	}
}

type TestTransportFunc func(req *http.Request) (*http.Response, error)

func (f TestTransportFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

// NewTestClient returns *http.Client with Transport replaced to avoid making real calls
func NewTestClient(fn TestTransportFunc) *http.Client {
	return &http.Client{
		Transport: TestTransportFunc(fn),
	}
}

// This is the "test" that is being run when the fakeAIEngine Cmd is being exec'ed
// We can validate the expected arguments and return the expected response
func TestAIEngineMockCmd(t *testing.T) {
	// If we are being called from the normal test suite, we just want to return immediately
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}

	switch os.Getenv("SCENARIO") {
	case "":
		fallthrough
	case "HAPPY_PATH":
		return
	}
}

func EnsureTestSpiceDirectory(t *testing.T) {
	// Ensure test config directory doesn't exist already so we don't hose it on cleanup
	_, err := os.Stat(constants.DotSpice)
	if err == nil {
		t.Errorf(".spice directory already exists")
		return
	}

	podsPath := filepath.Join(constants.DotSpice, "pods")
	err = os.MkdirAll(podsPath, 0766)
	if err != nil {
		t.Error(err)
		return
	}
}

func CleanupTestSpiceDirectory() {
	err := os.RemoveAll(constants.DotSpice)
	if err != nil {
		fmt.Println(err)
	}
}
