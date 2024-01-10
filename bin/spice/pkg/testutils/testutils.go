package testutils

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/spiceai/spiceai/bin/spice/pkg/constants"
)

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
