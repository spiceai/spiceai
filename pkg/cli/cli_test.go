package cli_test

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/spiceai/spice/pkg/cli/cmd"
	"github.com/spiceai/spice/pkg/pods"
	"github.com/stretchr/testify/assert"
)

func TestCLI(t *testing.T) {
	t.Cleanup(cleanup)

	// These tests simulate a real workflow and should run in sequence
	t.Run("podInitCmd() - spice pod init foo creates a skeleton pod", testPodInit(cmd.RootCmd))
	t.Run("actionAddCmd() - spice action add jump adds an action", testActionAddCmd(cmd.RootCmd))
	t.Run("rewardsAddCmd() - spice rewards add adds default rewards", testRewardsAddCmd(cmd.RootCmd))
}

func init() {
	// Ensure test config directory doesn't exist already so we don't hose it on cleanup
	_, err := os.Stat(".spice")
	if err == nil {
		fmt.Println(".spice directory already exists")
		os.Exit(1)
	}

	err = os.MkdirAll(".spice", 0766)
	if err != nil {
		fmt.Print(err.Error())
		os.Exit(1)
	}

	// Add all subcommands
	cmd.Execute()
}

func cleanup() {
	err := os.RemoveAll(".spice")
	if err != nil {
		fmt.Print(err.Error())
		os.Exit(1)
	}
}

func executeCommand(root *cobra.Command, args ...string) (output string, err error) {
	buf := new(bytes.Buffer)
	root.SetOut(buf)
	root.SetErr(buf)
	root.SetArgs(args)

	err = root.Execute()
	if err != nil {
		fmt.Println(err)
	}

	return buf.String(), err
}

// Tests pod init creates a loadable skeleton pod
func testPodInit(root *cobra.Command) func(*testing.T) {
	return func(t *testing.T) {
		_, err := executeCommand(root, "pod", "init", "foo")
		assert.NoError(t, err)
		_, err = os.Stat(".spice/pods/foo.yaml")
		assert.NoError(t, err)

		_, err = pods.LoadPodFromManifest(".spice/pods/foo.yaml")
		assert.NoError(t, err)
	}
}

// Tests action add adds an action
func testActionAddCmd(root *cobra.Command) func(*testing.T) {
	return func(t *testing.T) {
		_, err := executeCommand(root, "action", "add", "jump")
		assert.NoError(t, err)

		pod, err := pods.LoadPodFromManifest(".spice/pods/foo.yaml")
		assert.NoError(t, err)

		assert.Contains(t, pod.Actions(), "jump")
	}
}

// Tests rewards add adds default rewards
func testRewardsAddCmd(root *cobra.Command) func(*testing.T) {
	return func(t *testing.T) {
		_, err := executeCommand(root, "rewards", "add")
		assert.NoError(t, err)

		pod, err := pods.LoadPodFromManifest(".spice/pods/foo.yaml")
		assert.NoError(t, err)

		assert.Contains(t, pod.Rewards(), "jump")
	}
}
