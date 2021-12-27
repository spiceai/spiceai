package flights

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/logrusorgru/aurora"
	"github.com/spiceai/spiceai/pkg/util"
)

type Flight struct {
	id string

	algorithm string
	loggers   []string
	logDir    string

	start time.Time
	end   time.Time

	episodesMutex sync.RWMutex
	episodes      []*Episode

	isDone chan bool
	err    error
}

func NewFlight(id string, episodes int64, algorithm string, loggers []string, logDir string) (*Flight, error) {
	f := &Flight{
		id:        id,
		algorithm: algorithm,
		loggers:   loggers,
		start:     time.Now(),
		episodes:  make([]*Episode, 0, episodes),
		isDone:    make(chan bool, 1),
		err:       nil,
	}

	if len(loggers) > 0 {
		path := filepath.Join(logDir, "runs", fmt.Sprintf("run_%s", id))
		_, err := util.MkDirAllInheritPerm(path)
		if err != nil {
			return nil, fmt.Errorf("failed to create temp log dir %s: %w", path, err)
		}
		f.logDir = path
	}

	return f, nil
}

func (f *Flight) Id() string {
	return f.id
}

func (f *Flight) LogDir() string {
	return f.logDir
}

func (f *Flight) Algorithm() string {
	return f.algorithm
}

func (f *Flight) Loggers() []string {
	return f.loggers
}

func (f *Flight) WaitForDoneChan() *chan bool {
	return &f.isDone
}

func (f *Flight) RecordEpisode(e *Episode) {
	f.episodesMutex.Lock()
	defer f.episodesMutex.Unlock()

	f.episodes = append(f.episodes, e)

	if len(f.episodes) >= f.ExpectedEpisodes() || e.Error != "" {
		go func() {
			var err error = nil
			if e.Error != "" {
				err = fmt.Errorf("%s: %s", e.Error, e.ErrorMessage)
			}
			f.complete(err)
		}()
	}
}

func (f *Flight) Episodes() []*Episode {
	return f.episodes
}

func (f *Flight) GetEpisode(episodeId int64) *Episode {
	for _, e := range f.Episodes() {
		if e.EpisodeId == episodeId {
			return e
		}
	}

	return nil
}

func (f *Flight) ExpectedEpisodes() int {
	return cap(f.episodes)
}

func (f *Flight) Start() time.Time {
	return f.start
}

func (f *Flight) End() time.Time {
	return f.end
}

func (f *Flight) IsComplete() bool {
	return !f.end.IsZero()
}

func (f *Flight) Duration() time.Duration {
	if !f.end.IsZero() {
		return f.end.Sub(f.start)
	}

	return time.Since(f.start)
}

func (f *Flight) Close() error {
	return os.RemoveAll(f.logDir)
}

func (f *Flight) complete(err error) {
	f.end = time.Now()
	f.err = err
	if err != nil {
		fmt.Printf("Training run '%s' stopped on episode %d with error: %s\n", f.id, len(f.Episodes())+1, aurora.Red(err))
	}
	f.isDone <- true
}
