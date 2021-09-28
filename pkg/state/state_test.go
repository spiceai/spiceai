package state_test

import (
	"testing"

	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/spiceai/spiceai/pkg/state"
	"github.com/stretchr/testify/assert"
)

func TestContext(t *testing.T) {
	t.Run("NewState() - NewState and getters", testNewState())
}

// Tests NewState() creates State correctly with valid getter values
func testNewState() func(*testing.T) {
	return func(t *testing.T) {
		expectedPath := "test.path"
		expectedFieldNames := []string{"field1", "field2", "field3"}
		expectedTags := []string{}
		expectedObservations := []observations.Observation{}

		newState := state.NewState(expectedPath, expectedFieldNames, expectedTags, expectedObservations)

		assert.Equal(t, expectedPath, newState.Path(), "Path() not equal")
		assert.Equal(t, expectedFieldNames, newState.FieldNames(), "FieldNames() not equal")

		expectedFields := []string{"test.path.field1", "test.path.field2", "test.path.field3"}

		assert.Equal(t, expectedFields, newState.Fields(), "Fields() not equal")
	}
}
