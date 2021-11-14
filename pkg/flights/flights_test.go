package flights_test

import (
	"testing"
	"time"

	"github.com/spiceai/spiceai/pkg/flights"
	"github.com/stretchr/testify/assert"
)

func TestFlight(t *testing.T) {
	t.Run("testRecordEpisode() -- Should properly record an episode and complete", testRecordEpisode())
}

func testRecordEpisode() func(*testing.T) {
	return func(t *testing.T) {
		flight := flights.NewFlight("test", 1, "vpg")
		episode := flights.Episode{
			EpisodeId: 1,
		}

		ts := time.Now()
		flight.RecordEpisode(&episode)
		episodes := flight.Episodes()

		assert.Equal(t, 1, len(episodes), "unexpected number of episodes")
		assert.EqualValues(t, 1, episodes[0].EpisodeId)

		<-*flight.WaitForDoneChan()

		assert.True(t, flight.End() == ts || flight.End().After(ts))
	}
}
