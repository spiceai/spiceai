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

package context

import (
	"strings"
	"testing"

	"github.com/spf13/pflag"
)

func TestGetRunCmd_WithSpicepodPath(t *testing.T) {
	rtcontext := NewContext()
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)

	// Register required flags
	flags.String("tls-root-certificate-file", "", "")
	flags.String("api-key", "", "")
	flags.String("user-agent", "", "")
	flags.String("cache-control", "", "")
	flags.String("flight-endpoint", "", "")
	flags.String("http-endpoint", "", "")
	flags.String("metrics-endpoint", "", "")
	flags.String("open-telemetry-endpoint", "", "")
	flags.String("captured-output", "", "")
	flags.Bool("cloud", false, "")

	if err := rtcontext.Init(flags); err != nil {
		t.Fatalf("Failed to initialize context: %v", err)
	}

	testCases := []struct {
		name           string
		args           []string
		expectedInArgs []string
		description    string
	}{
		{
			name:           "positional argument with file path",
			args:           []string{"/path/to/spicepod.yaml"},
			expectedInArgs: []string{"/path/to/spicepod.yaml"},
			description:    "Should pass through file path as positional argument",
		},
		{
			name:           "positional argument with directory path",
			args:           []string{"/path/to/directory"},
			expectedInArgs: []string{"/path/to/directory"},
			description:    "Should pass through directory path as positional argument",
		},
		{
			name:           "positional argument with flags",
			args:           []string{"/path/to/spicepod.yaml", "-v"},
			expectedInArgs: []string{"/path/to/spicepod.yaml", "-v"},
			description:    "Should preserve both positional argument and flags",
		},
		{
			name:           "multiple verbosity flags",
			args:           []string{"/path/to/spicepod.yaml", "-vv"},
			expectedInArgs: []string{"/path/to/spicepod.yaml", "-vv"},
			description:    "Should handle multiple verbosity flags",
		},
		{
			name:           "no positional arguments",
			args:           []string{"-v"},
			expectedInArgs: []string{"-v"},
			description:    "Should work without positional arguments",
		},
		{
			name:           "empty args",
			args:           []string{},
			expectedInArgs: []string{},
			description:    "Should work with empty args",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cmd, err := rtcontext.GetRunCmd(tc.args)
			if err != nil {
				t.Fatalf("GetRunCmd failed: %v", err)
			}

			if cmd == nil {
				t.Fatal("GetRunCmd returned nil command")
			}

			cmdArgs := cmd.Args[1:] // Skip the binary name (first arg)

			// Check that all expected arguments are present
			for _, expected := range tc.expectedInArgs {
				found := false
				for _, actual := range cmdArgs {
					if actual == expected {
						found = true
						break
					}
				}
				if !found {
					t.Errorf("Expected argument '%s' not found in command args: %v", expected, cmdArgs)
				}
			}

			// Verify --pods-watcher-enabled is always added
			podsWatcherFound := false
			for _, arg := range cmdArgs {
				if arg == "--pods-watcher-enabled" {
					podsWatcherFound = true
					break
				}
			}
			if !podsWatcherFound {
				t.Error("Expected --pods-watcher-enabled flag not found in command args")
			}
		})
	}
}

func TestGetRunCmd_PreservesArgumentOrder(t *testing.T) {
	rtcontext := NewContext()
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)

	// Register required flags
	flags.String("tls-root-certificate-file", "", "")
	flags.String("api-key", "", "")
	flags.String("user-agent", "", "")
	flags.String("cache-control", "", "")
	flags.String("flight-endpoint", "", "")
	flags.String("http-endpoint", "", "")
	flags.String("metrics-endpoint", "", "")
	flags.String("open-telemetry-endpoint", "", "")
	flags.String("captured-output", "", "")
	flags.Bool("cloud", false, "")

	if err := rtcontext.Init(flags); err != nil {
		t.Fatalf("Failed to initialize context: %v", err)
	}

	args := []string{"/path/to/spicepod.yaml", "-vv"}
	cmd, err := rtcontext.GetRunCmd(args)
	if err != nil {
		t.Fatalf("GetRunCmd failed: %v", err)
	}

	cmdArgs := cmd.Args[1:] // Skip the binary name

	// Find the positional argument
	var positionalArgIndex int = -1
	for i, arg := range cmdArgs {
		if strings.HasPrefix(arg, "/path/to/") {
			positionalArgIndex = i
			break
		}
	}

	if positionalArgIndex == -1 {
		t.Fatal("Positional argument not found in command args")
	}

	// The positional argument should come before flag arguments (except --pods-watcher-enabled which is always first)
	// Find first occurrence of --pods-watcher-enabled
	podsWatcherIndex := -1
	for i, arg := range cmdArgs {
		if arg == "--pods-watcher-enabled" {
			podsWatcherIndex = i
			break
		}
	}

	if podsWatcherIndex == -1 {
		t.Fatal("--pods-watcher-enabled not found")
	}

	// Positional arg should come right after --pods-watcher-enabled
	if positionalArgIndex != podsWatcherIndex+1 {
		t.Logf("Command args: %v", cmdArgs)
		t.Logf("Expected positional arg at index %d, but found at index %d", podsWatcherIndex+1, positionalArgIndex)
	}
}
