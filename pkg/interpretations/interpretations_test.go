package interpretations_test

import (
	"testing"
	"time"

	"github.com/spiceai/spiceai/pkg/interpretations"
	"github.com/stretchr/testify/assert"
)

func TestInterpretations(t *testing.T) {
	t.Run("Intervals()", testIntervalsFunc())
	t.Run("Get()", testGetInterpretationsFunc())
}

// Tests Intervals()
func testIntervalsFunc() func(*testing.T) {
	return func(t *testing.T) {
		epoch := time.Now()
		period := 7 * 24 * time.Hour // 7 days
		granularity := 1 * time.Hour // 1 hour
		
		store := interpretations.NewInterpretationsStore(epoch, period, granularity)

		var expectedIntervals int64 = 24 * 7
		actualIntervals := store.Intervals()
		assert.Equal(t, expectedIntervals, actualIntervals)
	}
}

// Tests Get()
func testGetInterpretationsFunc() func(*testing.T) {
	return func(t *testing.T) {
		epoch := time.Now()
		period := 500 * time.Second
		granularity := time.Second

		store := interpretations.NewInterpretationsStore(epoch, period, granularity)

		startRange := epoch.Add(100 * time.Second)
		endRange := epoch.Add(200 * time.Second)

		var allInterpretations []interpretations.Interpretation
		var inRangeInterpretations []interpretations.Interpretation

		beforeRangeInterpretation, err := interpretations.NewInterpretation(startRange.Add(-2*time.Second), startRange.Add(-1*time.Second), "before range")
		if err != nil {
			t.Error(err)
		}

		err = store.Add(beforeRangeInterpretation)
		assert.NoError(t, err)
		allInterpretations = append(allInterpretations, *beforeRangeInterpretation)

		endOnRangeInterpretation, err := interpretations.NewInterpretation(startRange.Add(-1*time.Second), startRange, "end on range")
		if err != nil {
			t.Error(err)
		}

		err = store.Add(endOnRangeInterpretation)
		assert.NoError(t, err)
		allInterpretations = append(allInterpretations, *endOnRangeInterpretation)
		inRangeInterpretations = append(inRangeInterpretations, *endOnRangeInterpretation)

		withinRangeInterpretation, err := interpretations.NewInterpretation(startRange.Add(1*time.Second), startRange.Add(2*time.Second), "within range")
		if err != nil {
			t.Error(err)
		}

		err = store.Add(withinRangeInterpretation)
		assert.NoError(t, err)
		allInterpretations = append(allInterpretations, *withinRangeInterpretation)
		inRangeInterpretations = append(inRangeInterpretations, *withinRangeInterpretation)

		aroundRangeInterpretation, err := interpretations.NewInterpretation(startRange.Add(-1*time.Second), endRange.Add(1*time.Second), "around range")
		if err != nil {
			t.Error(err)
		}

		err = store.Add(aroundRangeInterpretation)
		assert.NoError(t, err)
		allInterpretations = append(allInterpretations, *aroundRangeInterpretation)
		inRangeInterpretations = append(inRangeInterpretations, *aroundRangeInterpretation)

		startOnRangeInterpretation, err := interpretations.NewInterpretation(endRange, endRange.Add(1*time.Second), "start on range")
		if err != nil {
			t.Error(err)
		}

		err = store.Add(startOnRangeInterpretation)
		assert.NoError(t, err)
		allInterpretations = append(allInterpretations, *startOnRangeInterpretation)
		inRangeInterpretations = append(inRangeInterpretations, *startOnRangeInterpretation)

		startAfterRangeInterpretation, err := interpretations.NewInterpretation(endRange.Add(1*time.Second), endRange.Add(2*time.Second), "start after range")
		if err != nil {
			t.Error(err)
		}

		err = store.Add(startAfterRangeInterpretation)
		assert.NoError(t, err)
		allInterpretations = append(allInterpretations, *startAfterRangeInterpretation)

		assert.Equal(t, 6, len(allInterpretations))
		assert.Equal(t, allInterpretations, store.All())

		assert.Equal(t, 4, len(store.Get(startRange, endRange)))
		assert.Equal(t, inRangeInterpretations, store.Get(startRange, endRange))
	}
}
