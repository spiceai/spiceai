package util

import (
	"io"
	"io/ioutil"
	"os"
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

	err = ioutil.WriteFile(filePath, contentToWrite, fileStats.Mode())
	if err != nil {
		return err
	}

	return nil
}

// We have to manually swap out environment variables,
// as Viper's AutomaticEnv() doesn't work with Unmarshal() and the workarounds do not work for nested structures.
// See https://github.com/spf13/viper/issues/761
func ReplaceEnvVariablesFromPath(filePath string, envVarPrefix string) ([]byte, error) {
	content, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	
	contentString := string(content)
	for _, envVarValPair := range os.Environ() {
        if (strings.HasPrefix(envVarValPair, envVarPrefix)) {
			envVar := strings.Split(envVarValPair, "=")[0]
			contentString = strings.ReplaceAll(contentString, envVar, os.Getenv(envVar))
		}
    }

	return []byte(contentString), nil
}
