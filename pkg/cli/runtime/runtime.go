package runtime

import (
	"fmt"
	"log"
	"os"

	"github.com/spiceai/spice/pkg/context"
)

func Run(contextFlag string, manifestPath string) error {
	fmt.Println("Spice AI runtime starting...")

	rtcontext, err := context.NewContext(contextFlag)
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	err = rtcontext.Init()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(1)
	}

	shouldInstall := false
	var upgradeVersion string
	if installRequired := rtcontext.IsRuntimeInstallRequired(); installRequired {
		fmt.Println("The Spice AI runtime has not yet been installed.")
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

	cmd, err := rtcontext.GetRunCmd(manifestPath)
	if err != nil {
		return err
	}

	cmd.Stderr = os.Stderr
	cmd.Stdout = os.Stdout

	err = cmd.Start()
	if err != nil {
		return err
	}

	return cmd.Wait()
}
