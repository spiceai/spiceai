package flights

import (
	"fmt"
	"path/filepath"

	"github.com/spiceai/spiceai/pkg/flights/loggers"
)

type TrainingLogger interface {
	Name() string
	Open() (string, error)
}

func (f *Flight) LoadLogger(loggerId string) (TrainingLogger, error) {
	switch loggerId {
	case "tensorboard":
		logDir := filepath.Join(f.DataDir(), "log")
		return &loggers.TensorboardLogger{
			LogDir: logDir,
		}, nil
	default:
		return nil, fmt.Errorf("Invalid logger %s", loggerId)
	}
}