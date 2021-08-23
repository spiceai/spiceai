package file

import (
	"fmt"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/logrusorgru/aurora"
	"github.com/spiceai/spice/pkg/config"
)

const (
	FileConnectorName string = "file"
)

type FileConnector struct {
	path      string
	fileInfo  fs.FileInfo
	noWatch   bool
	data      []byte
	dataMutex sync.RWMutex
}

func NewFileConnector() *FileConnector {
	return &FileConnector{}
}

func (c *FileConnector) Init(params map[string]string) error {
	c.dataMutex = sync.RWMutex{}

	path := params["path"]
	if !filepath.IsAbs(path) {
		path = filepath.Join(config.AppPath(), path)
	}

	c.path = path
	c.noWatch = params["watch"] != "true"

	newFileInfo, err := os.Stat(c.path)
	if err == nil {
		_, err := c.loadFileData(newFileInfo)
		if err != nil {
			return err
		}
	}

	if !c.noWatch {
		c.watchPath()
	}

	return nil
}

func (c *FileConnector) FetchData(epoch time.Time, period time.Duration, interval time.Duration) ([]byte, error) {
	c.dataMutex.Lock()
	defer c.dataMutex.Unlock()

	newFileInfo, err := os.Stat(c.path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file '%s': %w", c.path, err)
	}

	if c.fileInfo == nil || newFileInfo.ModTime().After(c.fileInfo.ModTime()) {
		// Only load file if it's changed since last read
		return c.loadFileData(newFileInfo)
	}

	return c.data, nil
}

func (c *FileConnector) loadFileData(newFileInfo fs.FileInfo) ([]byte, error) {
	log.Printf("loading file '%s' ...", c.path)

	loadStartTime := time.Now()

	fileData, err := ioutil.ReadFile(c.path)
	if err != nil {
		return nil, fmt.Errorf("failed to open file '%s': %w", c.path, err)
	}

	c.data = fileData
	c.fileInfo = newFileInfo

	duration := time.Since(loadStartTime)

	log.Println(aurora.Green(fmt.Sprintf("loaded file '%s' in %.2f seconds ...", filepath.Base(c.path), duration.Seconds())))

	return fileData, nil
}

func (c *FileConnector) watchPath() {
	go func() {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			log.Println(fmt.Errorf("error starting '%s' watcher: %w", c.path, err))
		}
		defer watcher.Close()

		if err := watcher.Add(c.path); err != nil {
			log.Println(fmt.Errorf("error starting '%s' watcher: %w", c.path, err))
		}

		log.Println(fmt.Sprintf("watching '%s' for updates", c.path))

		for {
			select {
			case event := <-watcher.Events:
				err := c.processWatchNotifyEvent(event, c.path)
				if err != nil {
					log.Println(fmt.Errorf("error processing '%s' event %s: %w", c.path, event, err))
				}
			case err := <-watcher.Errors:
				log.Println(fmt.Errorf("error processing '%s': %w", c.path, err))
			}
		}
	}()
}

func (c *FileConnector) processWatchNotifyEvent(event fsnotify.Event, path string) error {
	switch event.Op {
	case fsnotify.Create:
		fallthrough
	case fsnotify.Write:
		file := filepath.Join(path, event.Name)
		newFileInfo, err := os.Stat(file)
		if err != nil {
			return fmt.Errorf("failed to open file '%s': %w", file, err)
		}
		_, err = c.loadFileData(newFileInfo)
		if err != nil {
			return err
		}
	case fsnotify.Remove:
		c.dataMutex.Lock()
		defer c.dataMutex.Unlock()
		c.fileInfo = nil
		c.data = nil
	}

	return nil
}
