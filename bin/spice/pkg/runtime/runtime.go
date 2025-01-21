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

package runtime

import (
	"fmt"
	"log/slog"
	"os"

	"github.com/spiceai/spiceai/bin/spice/pkg/constants"
	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

// Ensures the runtime is installed. Returns true if the runtime was installed or upgraded, false if it was already installed.
func EnsureInstalled(flavor constants.Flavor, autoUpgrade bool, allowAccelerator bool) (bool, error) {
	if !flavor.IsValid() {
		return false, fmt.Errorf("invalid flavor")
	}

	rtcontext := context.NewContext()
	err := rtcontext.Init()
	if err != nil {
		slog.Error("initializing runtime context", "error", err)
		os.Exit(1)
	}

	shouldInstall := false
	var upgradeVersion string
	if installRequired := rtcontext.IsRuntimeInstallRequired(); installRequired {
		slog.Info("Spice runtime installation required")
		shouldInstall = true
	} else {
		upgradeVersion, err = rtcontext.IsRuntimeUpgradeAvailable()
		if err != nil {
			slog.Warn("error checking for runtime upgrade", "error", err)
		} else if upgradeVersion != "" && autoUpgrade {
			shouldInstall = true
		}
	}

	if models, _ := rtcontext.ModelsFlavorInstalled(); !models && flavor == constants.FlavorAI {
		shouldInstall = true
	}

	if shouldInstall {
		err = rtcontext.InstallOrUpgradeRuntime(flavor, allowAccelerator)
		if err != nil {
			return shouldInstall, err
		}
	}

	return shouldInstall, nil
}

func Run(args []string) error {
	slog.Info("Checking for latest Spice runtime release...")
	rtcontext := context.NewContext()

	err := rtcontext.Init()
	if err != nil {
		slog.Error("initializing runtime context", "error", err)
		os.Exit(1)
	}

	_, err = EnsureInstalled(constants.FlavorDefault, false, true)
	if err != nil {
		return err
	}

	cmd, err := rtcontext.GetRunCmd(args)
	if err != nil {
		return err
	}

	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	slog.Info("Spice.ai runtime starting...")
	err = util.RunCommand(cmd)
	if err != nil {
		return err
	}

	return nil
}
