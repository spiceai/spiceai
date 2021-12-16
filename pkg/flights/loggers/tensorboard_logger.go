package loggers

import (
	"os/exec"
	"path/filepath"

	"github.com/spiceai/spiceai/pkg/context"
	"github.com/spiceai/spiceai/pkg/util"
)

type TensorboardLogger struct {
	LogDir string
}

func (t *TensorboardLogger) Name() string {
	return "TensorBoard"
}

func (l *TensorboardLogger) Open() error {
	rtcontext := context.CurrentContext()
	tensorboardCmd := filepath.Join(rtcontext.AIEngineBinDir(), "tensorboard")
	cmd := exec.Command(tensorboardCmd, "--logdir", l.LogDir)
	return util.RunCommand(cmd)
}
