package tempdir

import (
	"fmt"
	"os"
	"sync"
	"time"
)

var (
	mutex           sync.Mutex
	tempDirectories []string
)

func CreateTempDir(purpose string) (string, error) {
	tempDir := os.TempDir()
	pattern := fmt.Sprintf("spice_%s_%v_*", purpose, time.Now().Unix())

	path, err := os.MkdirTemp(tempDir, pattern)
	if err != nil {
		return "", err
	}

	mutex.Lock()
	tempDirectories = append(tempDirectories, path)
	mutex.Unlock()

	return path, nil
}

func RemoveAllCreatedTempDirectories() error {
	mutex.Lock()
	defer mutex.Unlock()

	for _, tempDir := range tempDirectories {
		err := os.RemoveAll(tempDir)
		if err != nil {
			return err
		}
	}

	return nil
}
