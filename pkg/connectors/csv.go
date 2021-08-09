package connectors

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/logrusorgru/aurora"
	"github.com/spiceai/spice/pkg/csv"
	"github.com/spiceai/spice/pkg/observations"
)

type CsvConnector struct {
	path      string
	dataCache []observations.Observation
	seeded    bool
}

func NewCsvConnector(params map[string]string) *CsvConnector {
	return &CsvConnector{
		path:   params["path"],
		seeded: false,
	}
}

func (c *CsvConnector) Initialize() error {
	c.watchPath()

	return nil
}

func (c *CsvConnector) FetchData(period time.Duration, interval time.Duration) ([]observations.Observation, error) {
	if !c.seeded {
		data, err := c.loadCsvData()
		if err != nil {
			return nil, err
		}
		c.seeded = true
		return data, nil
	}

	if c.dataCache != nil {
		data := c.dataCache
		c.dataCache = nil
		return data, nil
	}

	return nil, nil
}

func (c *CsvConnector) loadCsvData() ([]observations.Observation, error) {
	log.Printf("Loading csv data from '%s' ...", c.path)
	loadStartTime := time.Now()

	file, err := os.Open(c.path)
	if err != nil {
		return nil, fmt.Errorf("failed to open csv file '%s': %w", c.path, err)
	}

	newObservations, err := csv.ProcessCsv(file)
	if err != nil {
		return nil, fmt.Errorf("error processing %s: %w", c.path, err)
	}

	duration := time.Since(loadStartTime)

	log.Println(aurora.Green(fmt.Sprintf("Loaded %d records of data from '%s' in %.1f seconds ...", len(newObservations), filepath.Base(c.path), duration.Seconds())))

	return newObservations, nil
}

func (c *CsvConnector) watchPath() {
	go func() {
		watcher, err := fsnotify.NewWatcher()
		if err != nil {
			log.Println(fmt.Errorf("error starting '%s' watcher: %w", c.path, err))
		}
		defer watcher.Close()

		if err := watcher.Add(c.path); err != nil {
			log.Println(fmt.Errorf("error starting '%s' watcher: %w", c.path, err))
		}

		log.Println(fmt.Sprintf("watching '%s' for csv file updates", c.path))

		for {
			select {
			case event := <-watcher.Events:
				err := c.processWatchNotifyEvent(event)
				if err != nil {
					log.Println(fmt.Errorf("error processing '%s' event %s: %w", c.path, event, err))
				}
			case err := <-watcher.Errors:
				log.Println(fmt.Errorf("error processing '%s': %w", c.path, err))
			}
		}
	}()
}

func (c *CsvConnector) processWatchNotifyEvent(event fsnotify.Event) error {
	csvPath := event.Name
	ext := filepath.Ext(csvPath)
	if ext != ".csv" {
		// Ignore non-CSV files
		return nil
	}

	switch event.Op {
	case fsnotify.Create:
		data, err := c.loadCsvData()
		if err != nil {
			return err
		}
		c.dataCache = data
	case fsnotify.Write:
		// TODO: Handle difference only
		data, err := c.loadCsvData()
		if err != nil {
			return err
		}
		c.dataCache = data
	case fsnotify.Remove:
		// TODO
		break
	}

	return nil
}
