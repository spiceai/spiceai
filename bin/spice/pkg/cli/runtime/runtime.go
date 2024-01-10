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
