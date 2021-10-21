package api

import (
	"testing"

	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/spiceai/spiceai/pkg/state"
	"github.com/spiceai/spiceai/pkg/testutils"
)

func TestNewObservationsFromState(t *testing.T) {
	snapshotter := testutils.NewSnapshotter("../../test/assets/snapshots/api/observations")

	measurementNames := []string{"m-1", "m-2"}
	tags := []string{"a", "b", "c"}
	observations := []observations.Observation{
		{
			Time: 1634804788,
			Measurements: map[string]float64{
				"m-1": 123.567,
				"m-2": 567.890,
				"m-3": 9999,
			},
			Categories: map[string]string{
				"c-1": "blah",
				"c-2": "yo",
			},
			Tags: []string{"a", "c"},
		},
		{
			Time: 1634808475,
			Measurements: map[string]float64{
				"m-1": 4734.3434,
				"m-2": 453.23465,
				"m-3": 344.5463,
			},
			Categories: map[string]string{
				"c-1": "ya",
				"c-2": "yow",
			},
			Tags: []string{"b", "c"},
		},
	}

	s := state.NewState("my.test.path", measurementNames, tags, observations)

	apiObservation := NewObservationsFromState(s)

	snapshotter.SnapshotTJson(t, apiObservation)
}
