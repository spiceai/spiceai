/*
Copyright 2021-2024 The Spice.ai OSS Authors

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
	"log"
	"os"

	"github.com/spiceai/spiceai/bin/spice/pkg/context"
	"github.com/spiceai/spiceai/bin/spice/pkg/util"
)

func Run() error {
	fmt.Println("Spice.ai runtime starting...")

	rtcontext := context.NewContext()

	err := rtcontext.Init()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	shouldInstall := false
	var upgradeVersion string
	if installRequired := rtcontext.IsRuntimeInstallRequired(); installRequired {
		fmt.Println("The Spice.ai runtime has not yet been installed.")
		shouldInstall = true
	} else {
		upgradeVersion, err = rtcontext.IsRuntimeUpgradeAvailable()
		if err != nil {
			log.Printf("error checking for runtime upgrade: %s", err.Error())
		} else if upgradeVersion != "" {
			shouldInstall = true
		}
	}

	if shouldInstall {
		err = rtcontext.InstallOrUpgradeRuntime()
		if err != nil {
			return err
		}
	}

	cmd, err := rtcontext.GetRunCmd()
	if err != nil {
		return err
	}

	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	err = util.RunCommand(cmd)
	if err != nil {
		return err
	}

	return nil
}
