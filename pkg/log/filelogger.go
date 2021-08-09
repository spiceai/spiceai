package log

import (
	"bufio"
	"fmt"
	"os"
	"time"
)

type FileLogger struct {
	path   string
	file   *os.File
	writer *bufio.Writer
}

func NewFileLogger(path string) *FileLogger {
	return &FileLogger{
		path: path,
	}
}

func FormatTimestampedLogFileName(basename string) string {
	return fmt.Sprintf("%s-%s.log", basename, time.Now().UTC().Format("20060102T150405Z"))
}

func (logger *FileLogger) Open() error {
	file, err := os.OpenFile(logger.path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	logger.file = file
	logger.writer = bufio.NewWriter(file)

	return nil
}

func (logger *FileLogger) Close() {
	if logger.writer != nil {
		logger.writer.Flush()
		logger.writer = nil
	}
	if logger.file != nil {
		logger.file.Close()
		logger.file = nil
	}
}

func (logger *FileLogger) Writeln(text string) error {
	_, err := logger.writer.WriteString(fmt.Sprintln(text))
	if err != nil {
		return err
	}
	return nil
}
