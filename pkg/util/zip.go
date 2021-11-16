package util

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type ProcessFunc func([]byte) error

// Opens the zip archive, finds a file that matches "filename" and runs "processFunc" on the bytes
func ProcessAFileInZipArchive(zipArchive string, filename string, processFunc ProcessFunc) error {
	r, err := zip.OpenReader(zipArchive)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		if f.Name == filename {
			reader, err := f.Open()
			if err != nil {
				return err
			}
			defer reader.Close()

			contents, err := io.ReadAll(reader)
			if err != nil {
				return err
			}

			err = processFunc(contents)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func ExtractZipFileToDir(zipArchive string, targetDirectory string) error {
	r, err := zip.OpenReader(zipArchive)
	if err != nil {
		return err
	}
	defer r.Close()

	for _, f := range r.File {
		reader, err := f.Open()
		if err != nil {
			return err
		}
		defer reader.Close()

		if err := sanitizeExtractPath(f.Name, targetDirectory); err != nil {
			return err
		}

		if f.FileInfo().IsDir() {
			// Copy file mask from target directory
			stat, err := os.Stat(targetDirectory)
			if err != nil {
				return err
			}

			err = os.Mkdir(filepath.Join(targetDirectory, f.Name), stat.Mode())
			if err != nil {
				return err
			}
			continue
		}

		fileHandle, err := os.Create(filepath.Join(targetDirectory, f.Name))
		if err != nil {
			return err
		}
		defer fileHandle.Close()

		_, err = io.Copy(fileHandle, reader)
		if err != nil {
			return err
		}
	}

	return nil
}

func sanitizeExtractPath(filePath string, destination string) error {
	destpath := filepath.Join(destination, filePath)
	if !strings.HasPrefix(destpath, filepath.Clean(destination) + string(os.PathSeparator)) {
		return fmt.Errorf("%s: illegal file path", filePath)
	}
	return nil
}