package loggers

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

func NewFileLogger(name string, dotSpicePath string) (*zap.Logger, error) {
	logPath := filepath.Join(dotSpicePath, "log")
	if _, err := os.Stat(logPath); err != nil {
		rootStat, err := os.Stat(dotSpicePath)
		if err != nil {
			return nil, fmt.Errorf("failed to find runtime path '%s': %w", dotSpicePath, err)
		}

		if err = os.MkdirAll(logPath, rootStat.Mode().Perm()); err != nil {
			return nil, fmt.Errorf("failed to create log path '%s'", logPath)
		}
	}

	fileName := fmt.Sprintf("%s-%s.log", name, time.Now().UTC().Format("20060102T150405Z"))
	logFilePath := filepath.Join(logPath, fileName)

	_, err := os.Create(logFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file '%s': %w", logFilePath, err)
	}

	w := zapcore.AddSync(&lumberjack.Logger{
		Filename:   logFilePath,
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
