package connectors

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/logrusorgru/aurora"
	"github.com/spiceai/spice/pkg/config"
	"github.com/spiceai/spice/pkg/csv"
	"github.com/spiceai/spice/pkg/observations"
)

type CsvConnector struct {
	path             string
	stateCache       []observations.Observation
	stateCacheMutex  sync.RWMutex
	stateUpdatedTime time.Time
	lastFetchedTime  time.Time
}

func NewCsvConnector(params map[string]string) *CsvConnector {
	path := params["path"]
	if !filepath.IsAbs(path) {
		path = filepath.Join(config.AppPath(), path)
	}

	return &CsvConnector{
		path:            path,
		stateCacheMutex: sync.RWMutex{},
	}
}

func (c *CsvConnector) Type() string {
	return CsvConnectorId
}

func (c *CsvConnector) Initialize() error {
	_, err := os.Stat(c.path)
	if err == nil {
		data, err := c.loadCsvData()
		if err != nil {
			return err
		}

		c.updateStateCache(data)
	}

	c.watchPath()

	return nil
}

func (c *CsvConnector) FetchData(epoch time.Time, period time.Duration, interval time.Duration) ([]observations.Observation, error) {
	if c.stateUpdatedTime.IsZero() {
		return make([]observations.Observation, 0), nil
	}

	if !c.lastFetchedTime.IsZero() && c.lastFetchedTime.Unix() > c.stateUpdatedTime.Unix() {
		return make([]observations.Observation, 0), nil
	}

	c.stateCacheMutex.RLock()
	defer c.stateCacheMutex.RUnlock()

	startTime := epoch.Unix()
	endTime := epoch.Add(period).Unix()

	var filteredObservations []observations.Observation
	for _, obs := range c.stateCache {
		if obs.Time >= startTime && obs.Time <= endTime {
			filteredObservations = append(filteredObservations, obs)
		}
	}

	c.lastFetchedTime = time.Now()

	return filteredObservations, nil
}

func (c *CsvConnector) updateStateCache(observations []observations.Observation) {
	c.stateCacheMutex.Lock()
	defer c.stateCacheMutex.Unlock()

	c.stateCache = observations
	c.stateUpdatedTime = time.Now()
}

func (c *CsvConnector) loadCsvData() ([]observations.Observation, error) {
	log.Printf("Loading csv data from '%s' ...", c.path)
	loadStartTime := time.Now()

	file, err := os.Open(c.path)
	if err != nil {
		return nil, fmt.Errorf("failed to open csv file '%s': %w", c.path, err)
	}

	newState, err := csv.ProcessCsv(file)
	if err != nil {
		return nil, fmt.Errorf("error processing %s: %w", c.path, err)
	}

	duration := time.Since(loadStartTime)

	log.Println(aurora.Green(fmt.Sprintf("Loaded %d records of data from '%s' in %.1f seconds ...", len(newState), filepath.Base(c.path), duration.Seconds())))

	return newState, nil
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
		fallthrough
	case fsnotify.Write:
		data, err := c.loadCsvData()
		if err != nil {
			return err
		}
		c.updateStateCache(data)
	case fsnotify.Remove:
		c.stateCache = nil
	}

	return nil
}
