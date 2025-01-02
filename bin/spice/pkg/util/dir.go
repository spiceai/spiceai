/*
Copyright 2024-2025 The Spice.ai OSS Authors

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

package util

import (
	"errors"
	"os"
	"path/filepath"
)

func MkDirAllInheritPerm(path string) (os.FileMode, error) {
	var stat os.FileInfo
	var err error
	cwpath := path
	for {
		parent := filepath.Dir(cwpath)
		stat, err = os.Stat(parent)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				cwpath = parent
				continue
			}
			return 0, err
		}
		break
	}

	return stat.Mode().Perm(), os.MkdirAll(path, stat.Mode().Perm())
}
