package tempdir

import (
	"fmt"
	"os"
	"path/filepath"
	"time"
)

var tempDirectories []string

func CreateTempDir(purpose string) (string, error) {
	tempDir := os.TempDir()
	stat, err := os.Stat(tempDir)
	if err != nil {
		return "", nil
	}

	spiceDir := fmt.Sprintf("spice_%s_%v", purpose, time.Now().Unix())
	tempDir = filepath.Join(tempDir, spiceDir)

	err = os.Mkdir(tempDir, stat.Mode())
	if err != nil {
		return "", err
	}

	tempDirectories = append(tempDirectories, tempDir)

	return tempDir, nil
}

func RemoveAllCreatedTempDirectories() error {
	for _, tempDir := range tempDirectories {
		err := os.RemoveAll(tempDir)
		if err != nil {
			return err
		}
	}

	tempDirectories = make([]string, 0)

	return nil
}

func init() {
	tempDirectories = make([]string, 0)
}
