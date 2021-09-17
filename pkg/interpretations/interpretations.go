package interpretations

import (
	"fmt"
	"sync"
	"time"

	"github.com/spiceai/spiceai/pkg/proto/common_pb"
	"github.com/spiceai/spiceai/pkg/util"
)

type InterpretationsStore struct {
	epoch       time.Time
	endTime     time.Time
	period      time.Duration
	granularity time.Duration
	intervals   int64

	interpretationsMutex sync.RWMutex
	interpretations      []Interpretation
	timeIndex            *common_pb.IndexedInterpretations
}

func NewInterpretationsStore(epoch time.Time, period time.Duration, granularity time.Duration) *InterpretationsStore {
	intervals := util.NumIntervals(period, granularity)
	return &InterpretationsStore{
		epoch:       epoch,
		endTime:     epoch.Add(period),
		period:      period,
		granularity: granularity,
		intervals:   intervals,
		timeIndex: &common_pb.IndexedInterpretations{
			Index: make(map[int64]*common_pb.InterpretationIndices),
		},
	}
}

func (store *InterpretationsStore) Intervals() int64 {
	return store.intervals
}

func (store *InterpretationsStore) All() []Interpretation {
	return store.interpretations
}

func (store *InterpretationsStore) IndexedInterpretations() *common_pb.IndexedInterpretations {
	store.interpretationsMutex.RLock()
	defer store.interpretationsMutex.RUnlock()

	return store.timeIndex
}

func (store *InterpretationsStore) Get(start time.Time, end time.Time) []Interpretation {
	// naive linear filter - something smarter later
	store.interpretationsMutex.RLock()
	defer store.interpretationsMutex.RUnlock()

	var filteredInterpretations []Interpretation
	// At least one end falls within the range
	for _, i := range store.interpretations {
		if i.End().Before(start) {
			continue
		}
		if i.Start().After(end) {
			continue
		}
		filteredInterpretations = append(filteredInterpretations, i)
	}

	return filteredInterpretations
}

func (store *InterpretationsStore) Add(interpretation *Interpretation) error {
	if interpretation.Start().Before(store.epoch) {
		return fmt.Errorf("interpretation start '%s' must be same or later than pod epoch '%s'", interpretation.Start().String(), store.epoch.String())
	}

	if interpretation.End().After(store.endTime) {
		return fmt.Errorf("interpretation end '%s' must be same or before pod end '%s'", interpretation.End().String(), store.endTime.String())
	}

	store.interpretationsMutex.Lock()
	store.interpretations = append(store.interpretations, *interpretation)
	store.addToTimeIndex(interpretation)
	store.interpretationsMutex.Unlock()

	return nil
}

func (store *InterpretationsStore) addToTimeIndex(interpretation *Interpretation) {
	timeIndex := store.timeIndex
	pbInterpretation := newPbInterpretation(interpretation)
	pbInterpretationIndex := uint32(len(timeIndex.Interpretations))
	timeIndex.Interpretations = append(timeIndex.Interpretations, pbInterpretation)

	for i := interpretation.Start(); i.Before(interpretation.End()); i = i.Add(store.granularity) {
		index := i.Unix()
		if timeIndex.Index[index] == nil {
			timeIndex.Index[index] = &common_pb.InterpretationIndices{}
		}
		timeIndex.Index[index].Indicies = append(timeIndex.Index[index].Indicies, pbInterpretationIndex)
	}
}

func newPbInterpretation(interpretation *Interpretation) *common_pb.Interpretation {
	return &common_pb.Interpretation{
		Start:   interpretation.Start().Unix(),
		End:     interpretation.End().Unix(),
		Name:    interpretation.Name(),
		Actions: interpretation.Actions(),
		Tags:    interpretation.Tags(),
	}
}
