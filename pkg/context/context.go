package context

import (
	"fmt"
	"log"
	"os/exec"
	"strings"

	"github.com/spiceai/spiceai/pkg/context/docker"
	"github.com/spiceai/spiceai/pkg/context/metal"
)

type RuntimeContext interface {
	Name() string
	Init(isDevelopmentMode bool) error
	Version() (string, error)
	IsRuntimeInstallRequired() bool
	InstallOrUpgradeRuntime() error
	IsRuntimeUpgradeAvailable() (string, error)
	SpiceRuntimeDir() string
	AppDir() string
	PodsDir() string
	AIEngineDir() string
	AIEngineBirDir() string
	AIEnginePythonCmdPath() string
	GetRunCmd(manifestPath string) (*exec.Cmd, error)
	GetSpiceAppRelativePath(absolutePath string) string
}

var (
	currentContext RuntimeContext
)

func NewContext(context string) (RuntimeContext, error) {
	context = strings.ToLower(context)

	var contextToSet RuntimeContext
	switch context {
	case "docker":
		contextToSet = docker.NewDockerContext()
	case "metal":
		contextToSet = metal.NewMetalContext()
	default:
		return nil, fmt.Errorf("invalid context '%s'", context)
	}

	return contextToSet, nil
}

func SetDefaultContext() error {
	rtcontext, err := NewContext("metal")
	if err != nil {
		return err
	}

	err = rtcontext.Init(false)
	if err != nil {
		return err
	}

	SetContext(rtcontext)

	return nil
}

func SetContext(context RuntimeContext) {
	currentContext = context
}

func CurrentContext() RuntimeContext {
	if currentContext == nil {
		err := SetDefaultContext()
		if err != nil {
			log.Fatal(err)
		}
	}

	return currentContext
}
