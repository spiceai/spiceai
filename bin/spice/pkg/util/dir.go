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
