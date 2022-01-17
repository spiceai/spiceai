package dataspace

import (
	"fmt"

	"github.com/apache/arrow/go/v6/arrow/array"
	"github.com/spiceai/spiceai/pkg/observations"
)

// type Observation struct {
// 	Time         int64
// 	Identifiers  map[string]string
// 	Measurements map[string]float64
// 	Categories   map[string]string
// 	Tags         []string
// }

const (
	colTypeTime = iota
	colTypeIdentifier
	colTypeMeasurement
	colTypeCategory
	colTypeTag
)

type ColumnInfo struct {
	Type             int
	Name             string
	IntColumn        *array.Int64
	FloatColumn      *array.Float64
	StringColumn     *array.String
	StringListColumn *array.List
}

func ArrowToObservations(record array.Record) ([]observations.Observation, error) {
	if record == nil {
		return nil, nil
	}
	timeColumn := -1
	tagColumn := -1
	columnInfo := make([]ColumnInfo, record.NumCols())
	hasId := false
	hasMeasure := false
	hasCategory := false

	for colIndex, field := range record.Schema().Fields() {
		switch {
		case field.Name == "time":
			if timeColumn != -1 {
				return nil, fmt.Errorf("Found column names \"time\" twice (column %d and %d)", timeColumn, colIndex)
			}
			timeColumn = colIndex
			columnInfo[colIndex].Type = colTypeTime
			columnInfo[colIndex].Name = "time"
			columnInfo[colIndex].IntColumn = record.Column(colIndex).(*array.Int64)
		case len(field.Name) > 3 && field.Name[:3] == "id.":
			if !hasId {
				hasId = true
			}
			columnInfo[colIndex].Type = colTypeIdentifier
			columnInfo[colIndex].Name = field.Name
			columnInfo[colIndex].StringColumn = record.Column(colIndex).(*array.String)
		case len(field.Name) > 8 && field.Name[:8] == "measure.":
			if !hasMeasure {
				hasMeasure = true
			}
			columnInfo[colIndex].Type = colTypeMeasurement
			columnInfo[colIndex].Name = field.Name
			columnInfo[colIndex].FloatColumn = record.Column(colIndex).(*array.Float64)
		case len(field.Name) > 4 && field.Name[:4] == "cat.":
			if !hasCategory {
				hasCategory = true
			}
			columnInfo[colIndex].Type = colTypeCategory
			columnInfo[colIndex].Name = field.Name
			columnInfo[colIndex].StringColumn = record.Column(colIndex).(*array.String)
		case field.Name == "tags":
			if tagColumn != -1 {
				return nil, fmt.Errorf("Found column names \"tags\" twice (column %d and %d)", tagColumn, colIndex)
			}
			tagColumn = colIndex
			columnInfo[colIndex].Type = colTypeTag
			columnInfo[colIndex].Name = "tags"
			columnInfo[colIndex].StringListColumn = record.Column(colIndex).(*array.List)
		default:
			return nil, fmt.Errorf("Unrecognized column name: %s", field.Name)
		}
	}

	var result []observations.Observation
	var offsets []int32
	var tags *array.String
	pos := 0
	if tagColumn >= 0 {
		offsets = columnInfo[tagColumn].StringListColumn.Offsets()[1:]
		tags = columnInfo[tagColumn].StringListColumn.ListValues().(*array.String)
	}
	for rowIndex := 0; rowIndex < int(record.NumRows()); rowIndex++ {
		var observation observations.Observation
		if hasId {
			observation.Identifiers = make(map[string]string)
		}
		if hasMeasure {
			observation.Measurements = make(map[string]float64)
		}
		if hasCategory {
			observation.Categories = make(map[string]string)
		}
		if tagColumn >= 0 {
			if tags.IsValid(rowIndex) {
				for j := pos; j < int(offsets[rowIndex]); j++ {
					observation.Tags = append(observation.Tags, tags.Value(j))
				}
				pos = int(offsets[rowIndex])
			}
		}
		for _, colInfo := range columnInfo {
			switch colInfo.Type {
			case colTypeMeasurement:
				observation.Measurements[colInfo.Name[8:]] = colInfo.FloatColumn.Value(rowIndex)
			case colTypeTime:
				observation.Time = colInfo.IntColumn.Value(rowIndex)
			case colTypeCategory:
				observation.Categories[colInfo.Name[4:]] = colInfo.StringColumn.Value(rowIndex)
			case colTypeIdentifier:
				observation.Identifiers[colInfo.Name[3:]] = colInfo.StringColumn.Value(rowIndex)
			}
		}
		result = append(result, observation)
	}

	return result, nil
}
