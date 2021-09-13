package interpretations_test

import (
	"testing"
	"time"

	"github.com/spiceai/spiceai/pkg/interpretations"
	"github.com/stretchr/testify/assert"
)

func TestInterpretation(t *testing.T) {
	now := time.Now()
	t.Run("NewInterpretation(now, now)", testNewInterpretationSuccessFunc(now, now, "same start and end"))
	t.Run("NewInterpretation(now, now.Add(1))", testNewInterpretationSuccessFunc(now, now.Add(1), "end after start"))
	t.Run("NewInterpretation(now, now.Add(-1))", testNewInterpretationFailFunc(now, now.Add(-1), "end before start"))

	t.Run("AddActions()", testAddActionsFunc(now))
	t.Run("AddTags()", testAddTagsFunc(now))
}

func testNewInterpretationSuccessFunc(start time.Time, end time.Time, name string) func(t *testing.T) {
	return func(t *testing.T) {
		i, err := interpretations.NewInterpretation(start, end, name)
		assert.NoError(t, err)
		assert.Equal(t, start, i.Start())
		assert.Equal(t, end, i.End())
		assert.Equal(t, name, i.Name())
	}
}

func testNewInterpretationFailFunc(start time.Time, end time.Time, name string) func(t *testing.T) {
	return func(t *testing.T) {
		_, err := interpretations.NewInterpretation(start, end, name)
		assert.Error(t, err)
	}
}

func testAddActionsFunc(now time.Time) func(t *testing.T) {
	return func(t *testing.T) {
		i, err := interpretations.NewInterpretation(now, now, "test")
		assert.NoError(t, err)

		actions := []string{"a", "b", "c"}
		i.AddActions(actions...)

		assert.Equal(t, actions, i.Actions())
	}
}

func testAddTagsFunc(now time.Time) func(t *testing.T) {
	return func(t *testing.T) {
		i, err := interpretations.NewInterpretation(now, now, "test")
		assert.NoError(t, err)

		tags := []string{"a", "b", "c"}
		i.AddTags(tags...)

		assert.Equal(t, tags, i.Tags())
	}
}
