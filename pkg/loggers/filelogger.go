package loggers

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spiceai/spice/pkg/config"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

func NewFileLogger(name string) (*zap.Logger, error) {
	path := config.SpiceLogPath()
	if _, err := os.Stat(path); err != nil {
		runtimePath := config.SpiceRuntimePath()
		rootStat, err := os.Stat(config.SpiceRuntimePath())
		if err != nil {
			return nil, fmt.Errorf("failed to find runtime path '%s': %w", runtimePath, err)
		}

		if err = os.MkdirAll(path, rootStat.Mode().Perm()); err != nil {
			return nil, fmt.Errorf("failed to create log path '%s'", path)
		}
	}

	fileName := fmt.Sprintf("%s-%s.log", name, time.Now().UTC().Format("20060102T150405Z"))
	logPath := filepath.Join(path, fileName)

	_, err := os.Create(logPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file '%s': %w", logPath, err)
	}

	w := zapcore.AddSync(&lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    100, // megabytes
		MaxBackups: 3,
		MaxAge:     60, // days
	})
	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
		w,
		zap.DebugLevel,
	)

	return zap.New(core), nil
}
