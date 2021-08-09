package util

import (
	"io"
	"io/ioutil"
	"os"
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
