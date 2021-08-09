package flights_test

import (
	"testing"
	"time"

	"github.com/spiceai/spice/pkg/flights"
	"github.com/stretchr/testify/assert"
)

func TestFlight(t *testing.T) {
	t.Run("testRecordEpisode() -- Should properly record an episode and complete", testRecordEpisode())
}

func testRecordEpisode() func(*testing.T) {
	return func(t *testing.T) {
		flight := flights.NewFlight(1)
		episode := flights.Episode{EpisodeId: 1}

		ts := time.Now()
		flight.RecordEpisode(&episode)

		assert.EqualValues(t, 1, flight.Episodes()[0].EpisodeId)
		assert.True(t, flight.End().After(ts))
	}
}
