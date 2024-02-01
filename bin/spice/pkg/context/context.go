package context

import (
	"os/exec"

	"github.com/spiceai/spiceai/bin/spice/pkg/context/metal"
)

type RuntimeContext interface {
	Name() string
	Init() error
	Version() (string, error)
	IsRuntimeInstallRequired() bool
	InstallOrUpgradeRuntime() error
	IsRuntimeUpgradeAvailable() (string, error)
	SpiceRuntimeDir() string
	AppDir() string
	PodsDir() string
	GetRunCmd() (*exec.Cmd, error)
	GetSpiceAppRelativePath(absolutePath string) string
}

func NewContext() RuntimeContext {
	rtcontext := metal.NewMetalContext()
	if err := rtcontext.Init(); err != nil {
		panic(err)
	}
	return rtcontext
}
