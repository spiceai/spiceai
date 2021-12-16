package loggers

import (
	"os/exec"

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
	cmd := exec.Command(rtcontext.AIEngineBinDir(), "tensorboard", "--logdir", l.LogDir)
	return util.RunCommand(cmd)
}
