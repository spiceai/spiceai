package flights

import (
	"fmt"

	"github.com/spiceai/spiceai/pkg/flights/loggers"
)

type TrainingLogger interface {
	Name() string
	Open() (string, error)
}

func (f *Flight) LoadLogger(loggerId string) (TrainingLogger, error) {
	switch loggerId {
	case "tensorboard":
		return &loggers.TensorboardLogger{
			RunId:  f.Id(),
			LogDir: f.LogDir(),
		}, nil
	default:
		return nil, fmt.Errorf("Invalid logger %s", loggerId)
	}
}
