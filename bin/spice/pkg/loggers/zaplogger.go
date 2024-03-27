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
	"log"

	"github.com/spiceai/spiceai/bin/spice/pkg/util"
	"go.uber.org/zap"
)

var (
	zapLogger *zap.Logger
)

func ZapLogger() *zap.Logger {
	if zapLogger != nil {
		return zapLogger
	}

	var err error
	if util.IsDebug() {
		zapLogger, err = zap.NewDevelopment()
	} else {
		zapLogger, err = zap.NewProduction()
	}
	if err != nil {
		// Fall back to standard logging
		log.Println(fmt.Errorf("unable to create Zap logger: %w", err))
		return nil
	}

	return zapLogger
}

func ZapLoggerSync() {
	if zapLogger != nil {
		err := zapLogger.Sync()
		if err != nil {
			// Swallow errors in sync
			// https://github.com/uber-go/zap/issues/880
			return
		}
	}
}
