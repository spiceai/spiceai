package state

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v6/arrow"
	"github.com/apache/arrow/go/v6/arrow/array"
	"github.com/apache/arrow/go/v6/arrow/memory"
)

type State struct {
	Time               time.Time
	TimeSentToAIEngine time.Time
	origin             string
	identifierNames    []string
	measurementNames   []string
	categoryNames      []string
	tags               []string
	tagMap             map[string]bool
	record             array.Record
	recordsMutex       sync.RWMutex
}

type StateInfo struct {
	idFields            []arrow.Field
	idColumns           []array.Interface
	measurementFields   []arrow.Field
	measurementsColumns []array.Interface
	categoryFields      []arrow.Field
	categoryColumns     []array.Interface
}

type StateHandler func(state *State, metadata map[string]string) error

func NewState(origin string, record array.Record) *State {

	var identifierNames []string
	var measurementNames []string
	var categoryNames []string
	var tags []string
	tagMap := make(map[string]bool)

	for _, field := range record.Schema().Fields() {
		switch {
		case strings.HasPrefix(field.Name, "measure."):
			measurementNames = append(measurementNames, field.Name)
		case strings.HasPrefix(field.Name, "cat."):
			categoryNames = append(categoryNames, field.Name)
		case strings.HasPrefix(field.Name, "id."):
			identifierNames = append(identifierNames, field.Name)
		}
	}

	// TODO: parse record to add tags

	return &State{
		Time:               time.Now(),
		TimeSentToAIEngine: time.Time{},
		origin:             origin,
		identifierNames:    identifierNames,
		measurementNames:   measurementNames,
		categoryNames:      categoryNames,
		tags:               tags,
		tagMap:             tagMap,
		record:             record,
	}
}

func GetStatesFromRecord(record array.Record) []*State {
	timeCol := -1
	tagCol := -1
	stateInfoMap := make(map[string]*StateInfo)
	var origins []string

	for colIndex, field := range record.Schema().Fields() {
		if field.Name == "time" {
			timeCol = colIndex
			continue
		}
		if field.Name == "tags" {
			tagCol = colIndex
			continue
		}
		fieldInfo := strings.Split(field.Name, ".")
		if len(fieldInfo) >= 4 {
			origin := strings.Join(fieldInfo[1:3], ".")
			if _, ok := stateInfoMap[origin]; !ok {
				stateInfoMap[origin] = &StateInfo{}
				origins = append(origins, origin)
			}
			stateInfo := stateInfoMap[origin]
			switch fieldInfo[0] {
			case "measure":
				stateInfo.measurementFields = append(
					stateInfo.measurementFields, arrow.Field{Name: fieldInfo[0] + "." + strings.Join(fieldInfo[3:], "."), Type: field.Type})
				stateInfo.measurementsColumns = append(stateInfo.measurementsColumns, record.Column(colIndex))
			case "id":
				stateInfo.idFields = append(
					stateInfo.idFields, arrow.Field{Name: fieldInfo[0] + "." + strings.Join(fieldInfo[3:], "."), Type: field.Type})
				stateInfo.idColumns = append(stateInfo.idColumns, record.Column(colIndex))
			case "cat":
				stateInfo.categoryFields = append(
					stateInfo.categoryFields, arrow.Field{Name: fieldInfo[0] + "." + strings.Join(fieldInfo[3:], "."), Type: field.Type})
				stateInfo.categoryColumns = append(stateInfo.categoryColumns, record.Column(colIndex))
			}
		}
	}
	if timeCol == -1 {
		return []*State{}
	}

	sort.Strings(origins)

	var result []*State
	for _, origin := range origins {
		stateInfo := stateInfoMap[origin]
		fields := []arrow.Field{record.Schema().Field(timeCol)}
		fields = append(fields, stateInfo.idFields...)
		fields = append(fields, stateInfo.measurementFields...)
		fields = append(fields, stateInfo.categoryFields...)
		columns := []array.Interface{record.Column(timeCol)}
		columns = append(columns, stateInfo.idColumns...)
		columns = append(columns, stateInfo.measurementsColumns...)
		columns = append(columns, stateInfo.categoryColumns...)
		if tagCol >= 0 {
			fields = append(fields, record.Schema().Field(tagCol))
			columns = append(columns, record.Column(tagCol))
		}
		result = append(
			result,
			NewState(origin, array.NewRecord(arrow.NewSchema(fields, nil), columns, record.NumRows())))
	}
	return result
}

func (s *State) Origin() string {
	return s.origin
}

func (s *State) IdentifierNames() []string {
	return s.identifierNames
}

func (s *State) MeasurementNames() []string {
	return s.measurementNames
}

func (s *State) CategoryNames() []string {
	return s.categoryNames
}

func (s *State) Record() array.Record {
	return s.record
}

func (s *State) Tags() []string {
	return s.tags
}

func (s *State) Sent() {
	s.TimeSentToAIEngine = time.Now()
}

func (s *State) AddData(newRecord array.Record) {
	s.recordsMutex.Lock()
	defer s.recordsMutex.Unlock()

	// TODO: parse record to add tags

	pool := memory.NewGoAllocator()
	var cols []array.Interface
	for colIndex, col := range s.record.Columns() {
		newCol, _ := array.Concatenate([]array.Interface{col, newRecord.Column(colIndex)}, pool)
		cols = append(cols, newCol)
	}
	mergedRecord := array.NewRecord(s.Record().Schema(), cols, s.record.NumRows()+newRecord.NumRows())
	s.record.Release()
	newRecord.Release()
	s.record = mergedRecord
}
