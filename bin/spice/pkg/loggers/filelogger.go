/*
 Copyright 2024 Spice AI, Inc.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

      https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package loggers

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/spiceai/spiceai/bin/spice/pkg/util"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

var (
	createLogDirectoryMutex sync.RWMutex
)

func NewFileLogger(name string, dotSpicePath string) (*zap.Logger, error) {

	logPath, err := createLogDirectory(dotSpicePath)
	if err != nil {
		return nil, err
	}

	fileName := fmt.Sprintf("%s-%s.log", name, time.Now().UTC().Format("20060102T150405Z"))
	logFilePath := filepath.Join(logPath, fileName)

	_, err = os.Create(logFilePath)
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

func createLogDirectory(dotSpicePath string) (string, error) {
	createLogDirectoryMutex.Lock()
	defer createLogDirectoryMutex.Unlock()

	logPath := filepath.Join(dotSpicePath, "log")
	if _, err := util.MkDirAllInheritPerm(logPath); err != nil {
		return "", fmt.Errorf("failed to create log path '%s'", logPath)
	}

	return logPath, nil
}
