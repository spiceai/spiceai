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
	"archive/zip"
	"bytes"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func SaveReaderToFile(reader io.Reader, fullFilePath string) error {
	fileHandle, err := os.OpenFile(fullFilePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0766)
	if err != nil {
		return err
	}

	defer fileHandle.Close()

	_, err = io.Copy(fileHandle, reader)
	if err != nil {
		return err
	}

	return nil
}

func WriteToExistingFile(filePath string, contentToWrite []byte) error {
	fileStats, err := os.Stat(filePath)
	if err != nil {
		return err
	}

	err = os.WriteFile(filePath, contentToWrite, fileStats.Mode())
	if err != nil {
		return err
	}

	return nil
}

func ExtractZip(body []byte, downloadDir string) error {
	zipBytesReader := bytes.NewReader(body)

	zipReader, err := zip.NewReader(zipBytesReader, int64(len(body)))
	if err != nil {
		return err
	}

	for _, file := range zipReader.File {
		reader, err := file.Open()
		if err != nil {
			return err
		}

		defer reader.Close()

		fileName := file.FileInfo().Name()

		fileToWrite := filepath.Join(downloadDir, fileName)

		newFile, err := os.Create(fileToWrite)
		if err != nil {
			return err
		}

		defer newFile.Close()

		_, err = io.Copy(newFile, reader)
		if err != nil {
			return err
		}
	}

	return nil
}

func ExtractTarGzInsideZip(body []byte, downloadDir string) error {
	zipBytesReader := bytes.NewReader(body)
	zipReader, err := zip.NewReader(zipBytesReader, int64(len(body)))
	if err != nil {
		return err
	}

	for _, file := range zipReader.File {
		reader, err := file.Open()
		if err != nil {
			return err
		}

		defer reader.Close()

		tarFileName := file.FileInfo().Name()

		if !strings.HasSuffix(tarFileName, ".tar.gz") {
			return errors.New("Unexpected file: " + tarFileName)
		}

		err = Untar(reader, downloadDir, true)
		if err != nil {
			return err
		}
	}

	return nil
}

func ExtractTarGz(body []byte, downloadDir string) error {
	bodyReader := bytes.NewReader(body)
	err := Untar(bodyReader, downloadDir, true)
	if err != nil && err.Error() == "requires gzip-compressed body: gzip: invalid header" {
		_, err = bodyReader.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}
		return Untar(bodyReader, downloadDir, false)
	}
	return err
}

// We have to manually swap out environment variables,
// as Viper's AutomaticEnv() doesn't work with Unmarshal() and the workarounds do not work for nested structures.
// See https://github.com/spf13/viper/issues/761
func ReplaceEnvVariablesFromPath(filePath string, envVarPrefix string) ([]byte, error) {
	content, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	contentString := string(content)
	for _, envVarValPair := range os.Environ() {
		if strings.HasPrefix(envVarValPair, envVarPrefix) {
			envVar := strings.Split(envVarValPair, "=")[0]
			contentString = strings.ReplaceAll(contentString, envVar, os.Getenv(envVar))
		}
	}

	return []byte(contentString), nil
}

func MakeFileExecutable(filepath string) error {
	return os.Chmod(filepath, 0777)
}

func CopyFile(src string, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}

	perm, err := MkDirAllInheritPerm(filepath.Dir(dst))
	if err != nil {
		return err
	}

	return os.WriteFile(dst, data, perm)
}
