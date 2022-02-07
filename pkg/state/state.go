package state

import (
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/memory"
)

type State struct {
	Time               time.Time
	TimeSentToAIEngine time.Time
	origin             string
	identifierNames    []string
	measurementNames   []string
	categoryNames      []string
	columnMap          map[string]int
	tags               []string
	tagMap             map[string]bool
	record             arrow.Record
	recordsMutex       sync.RWMutex
}

type StateInfo struct {
	idFields            []arrow.Field
	idColumns           []arrow.Array
	measurementFields   []arrow.Field
	measurementsColumns []arrow.Array
	categoryFields      []arrow.Field
	categoryColumns     []arrow.Array
}

type StateHandler func(state *State, metadata map[string]string) error

func NewState(origin string, record arrow.Record) *State {
	if record == nil {
		return nil
	}

	var identifierNames []string
	var measurementNames []string
	var categoryNames []string
	columnMap := make(map[string]int)
	var tags []string
	tagMap := make(map[string]bool)

	for columnIndex, field := range record.Schema().Fields() {
		columnMap[field.Name] = columnIndex
		switch {
		case strings.HasPrefix(field.Name, "measure."):
			measurementNames = append(measurementNames, field.Name)
		case strings.HasPrefix(field.Name, "cat."):
			categoryNames = append(categoryNames, field.Name)
			columnMap[field.Name] = columnIndex
		case strings.HasPrefix(field.Name, "id."):
			identifierNames = append(identifierNames, field.Name)
		}
	}

	newState := State{
		Time:               time.Now(),
		TimeSentToAIEngine: time.Time{},
		origin:             origin,
		identifierNames:    identifierNames,
		measurementNames:   measurementNames,
		categoryNames:      categoryNames,
		columnMap:          columnMap,
		tags:               tags,
		tagMap:             tagMap,
		record:             record,
	}
	newState.addRecordTags(record)
	return &newState
}

func GetStatesFromRecord(record arrow.Record) []*State {
	if record == nil {
		return []*State{}
	}

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
		columns := []arrow.Array{record.Column(timeCol)}
		columns = append(columns, stateInfo.idColumns...)
		columns = append(columns, stateInfo.measurementsColumns...)
		columns = append(columns, stateInfo.categoryColumns...)
		if tagCol >= 0 {
			fields = append(fields, record.Schema().Field(tagCol))
			columns = append(columns, record.Column(tagCol))
		}
		newRecord := array.NewRecord(arrow.NewSchema(fields, nil), columns, record.NumRows())
		newRecord.Retain()
		result = append(result, NewState(origin, newRecord))
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

func (s *State) ColumnMap() map[string]int {
	return s.columnMap
}

func (s *State) Record() *arrow.Record {
	return &s.record
}

func (s *State) Tags() []string {
	return s.tags
}

func (s *State) Sent() {
	s.TimeSentToAIEngine = time.Now()
}

func (s *State) AddData(newRecord arrow.Record) {
	s.recordsMutex.Lock()
	defer s.recordsMutex.Unlock()

	s.addRecordTags(newRecord)

	pool := memory.NewGoAllocator()
	var cols []arrow.Array
	for colIndex, col := range s.record.Columns() {
		newCol, _ := array.Concatenate([]arrow.Array{col, newRecord.Column(colIndex)}, pool)
		cols = append(cols, newCol)
	}
	mergedRecord := array.NewRecord((*s.Record()).Schema(), cols, s.record.NumRows()+newRecord.NumRows())
	s.record.Release()
	newRecord.Release()
	s.record = mergedRecord
}

func (s *State) addRecordTags(record arrow.Record) {
	tagCol := record.Column(int(record.NumCols() - 1)).(*array.List)
	tagOffsets := tagCol.Offsets()[1:]
	tagValues := tagCol.ListValues().(*array.String)
	tagPos := 0
	for rowIndex := 0; rowIndex < int(record.NumRows()); rowIndex++ {
		if tagValues.IsValid(rowIndex) {
			for tagPos < int(tagOffsets[rowIndex]) {
				tagValue := tagValues.Value(tagPos)
				if _, ok := s.tagMap[tagValue]; !ok {
					s.tagMap[tagValue] = true
					s.tags = append(s.tags, tagValue)
				}
				tagPos++
			}
		}
	}
}
