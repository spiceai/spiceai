package api

import (
	"os"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/stretchr/testify/assert"
)

var snapshotter = cupaloy.New(cupaloy.SnapshotSubdirectory("../../test/assets/snapshots/api/observation"))

func TestObservation(t *testing.T) {
	data, err := os.ReadFile("../../test/assets/data/json/observations.json")
	if err != nil {
		t.Fatal(err.Error())
	}

	t.Run("GetSchema()", testGetSchemaFunc())
	t.Run("GetObservations()", testGetObservationsFunc(data))
}

// Tests "GetSchema()"
func testGetSchemaFunc() func(*testing.T) {
	p := &ObservationJsonFormat{}

	return func(t *testing.T) {
		schema := p.GetSchema()
		assert.NotNil(t, schema)

		snapshotter.SnapshotT(t, schema)
	}
}

// Tests "GetObservations()"
func testGetObservationsFunc(data []byte) func(*testing.T) {
	return func(t *testing.T) {
		if len(data) == 0 {
			t.Fatal("no data")
		}

		p := &ObservationJsonFormat{}

		actualObservations, err := p.GetObservations(&data)
		if err != nil {
			t.Error(err)
			return
		}

		expectedFirstObservation := observations.Observation{
			Time: 1605312000,
			Data: map[string]float64{
				"eventId": 806.42,
				"height":  29,
				"rating":  86,
				"speed":   15,
				"target":  42,
			},
		}
		assert.Equal(t, expectedFirstObservation, actualObservations[0], "First Observation not correct")

		snapshotter.SnapshotT(t, actualObservations)
	}
}
