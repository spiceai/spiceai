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
	"archive/tar"
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestUntarPreventsPathTraversal(t *testing.T) {
	tarData := createTarArchive(t, map[string]string{
		"../escape.txt": "malicious content",
	})

	targetDir := t.TempDir()
	err := Untar(bytes.NewReader(tarData), targetDir, false)

	assert.Error(t, err)
	_, statErr := os.Stat(filepath.Join(targetDir, "..", "escape.txt"))
	assert.True(t, os.IsNotExist(statErr), "expected no file to be written outside target directory")
}

func TestUntarRejectsAbsolutePath(t *testing.T) {
	tarData := createTarArchive(t, map[string]string{
		"/escape.txt": "malicious content",
	})

	targetDir := t.TempDir()
	err := Untar(bytes.NewReader(tarData), targetDir, false)

	assert.Error(t, err)
	entries, readErr := os.ReadDir(targetDir)
	require.NoError(t, readErr)
	assert.Empty(t, entries, "expected target directory to remain empty")
}

func TestUntarExtractsWithinTargetDirectory(t *testing.T) {
	const (
		filePath = "nested/file.txt"
		content  = "safe content"
	)

	tarData := createTarArchive(t, map[string]string{
		filePath: content,
	})

	targetDir := t.TempDir()
	err := Untar(bytes.NewReader(tarData), targetDir, false)

	require.NoError(t, err)
	extractedBytes, readErr := os.ReadFile(filepath.Join(targetDir, filePath))
	require.NoError(t, readErr)
	assert.Equal(t, content, string(extractedBytes))
}

func createTarArchive(t *testing.T, entries map[string]string) []byte {
	t.Helper()

	var buffer bytes.Buffer
	tw := tar.NewWriter(&buffer)

	for name, content := range entries {
		data := []byte(content)
		header := &tar.Header{
			Name: name,
			Mode: 0o600,
			Size: int64(len(data)),
		}

		require.NoError(t, tw.WriteHeader(header))

		_, writeErr := tw.Write(data)
		require.NoError(t, writeErr)
	}

	require.NoError(t, tw.Close())

	return buffer.Bytes()
}
