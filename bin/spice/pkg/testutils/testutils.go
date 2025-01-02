/*
Copyright 2024-2025 The Spice.ai OSS Authors

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
