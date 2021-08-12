package flights

import (
	"sync"
	"time"
)

type Flight struct {
	start    time.Time
	end      time.Time
	episodes []*Episode
	episodesMutex sync.RWMutex
}

func NewFlight(episodes int) *Flight {
	return &Flight{
		start:    time.Now(),
		episodes: make([]*Episode, 0, episodes),
	}
}

func (f *Flight) Complete() {
	f.end = time.Now()
}

func (f *Flight) RecordEpisode(e *Episode) {
	f.episodesMutex.Lock()
	defer f.episodesMutex.Unlock()

	f.episodes = append(f.episodes, e)
	if len(f.episodes) >= f.ExpectedEpisodes() || e.Error != "" {
		f.Complete()
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

func (f *Flight) Duration() time.Duration {
	if !f.end.IsZero() {
		return f.end.Sub(f.start)
	}

	return time.Since(f.start)
}
