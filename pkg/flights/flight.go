package flights

import (
	"fmt"
	"sync"
	"time"

	"github.com/logrusorgru/aurora"
)

type Flight struct {
	id            string
	algorithm     string
	start         time.Time
	end           time.Time
	episodes      []*Episode
	episodesMutex sync.RWMutex
	isDone        chan bool
	err           error
}

func NewFlight(id string, episodes int, algorithm string) *Flight {
	return &Flight{
		id:        id,
		algorithm: algorithm,
		start:     time.Now(),
		episodes:  make([]*Episode, 0, episodes),
		isDone:    make(chan bool, 1),
		err:       nil,
	}
}

func (f *Flight) Algorithm() string {
	return f.algorithm
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

func (f *Flight) complete(err error) {
	f.end = time.Now()
	f.err = err
	if err != nil {
		fmt.Printf("Training run '%s' stopped on episode %d with error: %s\n", f.id, len(f.Episodes())+1, aurora.Red(err))
	}
	f.isDone <- true
}
