package util

import (
	"errors"
	"os"
	"path/filepath"
)

func MkDirAllInheritPerm(path string) error {
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
			return err
		}
		break
	}

	return os.MkdirAll(path, stat.Mode())
}
