package pods

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v7/arrow"
	"github.com/apache/arrow/go/v7/arrow/array"
	"github.com/apache/arrow/go/v7/arrow/csv"
	"github.com/apache/arrow/go/v7/arrow/memory"
	"github.com/spf13/viper"
	"github.com/spiceai/spiceai/pkg/constants"
	"github.com/spiceai/spiceai/pkg/dataspace"
	"github.com/spiceai/spiceai/pkg/flights"
	"github.com/spiceai/spiceai/pkg/interpretations"
	"github.com/spiceai/spiceai/pkg/spec"
	"github.com/spiceai/spiceai/pkg/state"
	"github.com/spiceai/spiceai/pkg/tempdir"
	spice_time "github.com/spiceai/spiceai/pkg/time"
	"github.com/spiceai/spiceai/pkg/util"
	"github.com/spiceai/spiceai/pkg/validator"
	"golang.org/x/sync/errgroup"
)

type Pod struct {
	spec.PodSpec
	viper        *viper.Viper
	podParams    *PodParams
	hash         string
	manifestPath string

	timeCategories    map[string][]spice_time.TimeCategoryInfo
	timeCategoryNames []string

	dataspaces          []*dataspace.Dataspace
	dataspaceMap        map[string]*dataspace.Dataspace
	actions             map[string]string
	measurements        map[string]*dataspace.MeasurementInfo
	fqIdentifierNames   []string
	fqMeasurementNames  []string
	fqCategoryNames     []string
	tags                []string
	externalRewardFuncs string

	flights map[string]*flights.Flight
	logDir  string

	podLocalStateMutex    sync.RWMutex
	podLocalState         []*state.State
	podLocalStateHandlers []state.StateHandler

	interpretations *interpretations.InterpretationsStore
}

func (pod *Pod) Hash() string {
	return pod.hash
}

func (f *Pod) ManifestPath() string {
	return f.manifestPath
}

func (pod *Pod) Period() time.Duration {
	return pod.podParams.Period
}

func (pod *Pod) Interval() time.Duration {
	return pod.podParams.Interval
}

func (pod *Pod) Epoch() time.Time {
	if pod.podParams.Epoch.IsZero() {
		return time.Now().Add(-pod.Period())
	}

	return pod.podParams.Epoch
}

func (pod *Pod) Granularity() time.Duration {
	return pod.podParams.Granularity
}

func (pod *Pod) Interpolation() bool {
	return pod.podParams.Interpolation
}

func (pod *Pod) TrainingLoggers() []string {
	if pod.PodSpec.Training != nil {
		return pod.PodSpec.Training.Loggers
	}
	return nil
}

func (pod *Pod) TimeCategories() map[string][]spice_time.TimeCategoryInfo {
	return pod.timeCategories
}

func (pod *Pod) TimeCategoryNames() []string {
	return pod.timeCategoryNames
}

func (pod *Pod) TrainingGoal() *string {
	if pod.PodSpec.Training == nil {
		return nil
	}

	return &pod.PodSpec.Training.Goal
}

func (pod *Pod) Episodes() int64 {
	if pod.PodSpec.Params != nil {
		episodesParam, ok := pod.PodSpec.Params["episodes"]
		if ok {
			if episodes, err := strconv.ParseInt(episodesParam, 0, 64); err == nil {
				return episodes
			}
		}
	}
	return 10
}

func (pod *Pod) CachedState() []*state.State {
	var cachedState []*state.State
	for _, ds := range pod.Dataspaces() {
		dsState := ds.CachedState()
		if dsState != nil {
			cachedState = append(cachedState, dsState...)
		}
	}

	if pod.podLocalState != nil {
		pod.podLocalStateMutex.RLock()
		defer pod.podLocalStateMutex.RUnlock()
		if pod.podLocalState != nil && len(pod.podLocalState) > 0 {
			cachedState = append(cachedState, pod.podLocalState...)
		}
	}

	return cachedState
}

func (pod *Pod) CachedRecord(csvTag bool) arrow.Record {
	cacheStates := pod.CachedState()

	// Mapping the states and grouping them by starting time (with the first state being the longest)
	stateMap := make(map[int64][]*state.State) // keep track of states from their starting time
	var startTimeList []int64
	for _, statePointer := range cacheStates {
		record := *statePointer.Record()
		if record.NumRows() == 0 {
			continue
		}
		startTime := record.Column(0).(*array.Int64).Value(0)
		stateList, ok := stateMap[startTime]
		if ok {
			if record.NumRows() > (*stateList[0].Record()).NumRows() {
				stateList = append([]*state.State{statePointer}, stateList...)
			} else {
				stateList = append(stateList, statePointer)
			}
			stateMap[startTime] = stateList
		} else {
			stateMap[startTime] = []*state.State{statePointer}
			startTimeList = append(startTimeList, startTime)
		}
	}
	sort.Slice(startTimeList, func(i, j int) bool { return startTimeList[i] < startTimeList[j] })

	// Initializing the scheme and list to be populated
	pool := memory.NewGoAllocator()
	fqFields := []arrow.Field{
		{Name: "time", Type: arrow.PrimitiveTypes.Int64},
	}
	var timeValues []arrow.Array
	timeValuesMap := make(map[string][]arrow.Array)
	measurementValuesMap := make(map[string][]arrow.Array)
	identifierValuesMap := make(map[string][]arrow.Array)
	categoryValuesMap := make(map[string][]arrow.Array)
	categoryOneHotMap := make(map[string]map[string]int) // map[fqCategoryName][value] = field index
	categoryOneHotNameMap := make(map[string][]string)   // map[fqCategoryNames] = [oneHotName1, oneHotName2, ...]
	for _, timeCategoryName := range pod.timeCategoryNames {
		for _, timeCategory := range pod.timeCategories[timeCategoryName] {
			fqFields = append(fqFields, arrow.Field{Name: timeCategory.FieldName, Type: arrow.PrimitiveTypes.Int8})
			timeValuesMap[timeCategory.FieldName] = []arrow.Array{}
		}
	}
	for _, idName := range pod.fqIdentifierNames {
		fqFields = append(fqFields, arrow.Field{Name: idName, Type: arrow.BinaryTypes.String})
		identifierValuesMap[idName] = []arrow.Array{}
	}
	for _, measurementName := range pod.fqMeasurementNames {
		fqFields = append(fqFields, arrow.Field{Name: measurementName, Type: arrow.PrimitiveTypes.Float64})
		measurementValuesMap[measurementName] = []arrow.Array{}
	}
	for _, dataspace := range pod.Dataspaces() {
		for _, category := range dataspace.Categories() {
			categoryOneHotMap[category.FqName] = make(map[string]int)
			categoryOneHotNameMap[category.FqName] = category.EncodedFieldNames
			for valueIndex, oneHotName := range category.EncodedFieldNames {
				categoryOneHotMap[category.FqName][category.Values[valueIndex]] = valueIndex
				fqFields = append(
					fqFields, arrow.Field{Name: oneHotName, Type: arrow.PrimitiveTypes.Int8})
				categoryValuesMap[oneHotName] = []arrow.Array{}
			}
		}
	}
	if csvTag {
		fqFields = append(fqFields, arrow.Field{Name: "tags", Type: arrow.BinaryTypes.String})
	} else {
		fqFields = append(fqFields, arrow.Field{Name: "tags", Type: arrow.ListOf(arrow.BinaryTypes.String)})
	}
	tagBuilder := array.NewStringBuilder(pool)
	defer tagBuilder.Release()
	tagListBuilder := array.NewListBuilder(pool, arrow.BinaryTypes.String)
	defer tagListBuilder.Release()
	tagValueBuilder := tagListBuilder.ValueBuilder().(*array.StringBuilder)

	// Accumulating values in the builder to create the merged record
	numRows := int64(0)
	for _, startTime := range startTimeList {
		parsedColumnMap := make(map[string]bool) // keep track of column already parsed for this starting time
		chunkLen := int64(0)
		for stateIndex, statePointer := range stateMap[startTime] {
			record := *statePointer.Record()
			if stateIndex == 0 { // appending time values for the first state (longest one)
				chunkLen = record.NumRows()
				numRows += chunkLen
				timeValues = append(timeValues, record.Column(0))
				for _, timeCategoryName := range pod.timeCategoryNames {
					recordTimeValues := record.Column(0).(*array.Int64)
					valueBuilders := make([]*array.Int8Builder, len(pod.timeCategories[timeCategoryName]))
					categoryValues := make([]int, len(valueBuilders))
					for valueIndex := range valueBuilders {
						valueBuilders[valueIndex] = array.NewInt8Builder(pool)
						categoryValues[valueIndex] = pod.timeCategories[timeCategoryName][valueIndex].Value
					}
					for i := int64(0); i < chunkLen; i++ {
						rowTime := time.Unix(recordTimeValues.Value(int(i)), 0).UTC()
						var rowValue int
						switch timeCategoryName {
						case spice_time.CategoryMonth:
							rowValue = int(rowTime.Month())
						case spice_time.CategoryDayOfMonth:
							rowValue = rowTime.Day()
						case spice_time.CategoryDayOfWeek:
							rowValue = int(rowTime.Weekday())
						case spice_time.CategoryHour:
							rowValue = rowTime.Hour()
						}
						for valueIndex, builder := range valueBuilders {
							if categoryValues[valueIndex] == int(rowValue) {
								builder.Append(1)
							} else {
								builder.Append(0)
							}
						}
					}
					for timeIndex, timeCategory := range pod.timeCategories[timeCategoryName] {
						valueList := timeValuesMap[timeCategory.FieldName]
						timeValuesMap[timeCategory.FieldName] = append(valueList, valueBuilders[timeIndex].NewArray())
						valueBuilders[timeIndex].Release()
					}
				}
			}
			for _, name := range (*statePointer).IdentifierNames() {
				fqName := (*statePointer).Origin() + "." + strings.Join(strings.Split(name, ".")[1:], ".")
				if parsedColumnMap[fqName] {
					fmt.Printf("Column already parsed during CSV generation at start time %d: %s\n", startTime, fqName)
					continue
				}
				valueList, ok := identifierValuesMap[fqName]
				if !ok {
					fmt.Printf("Identifier column not found during CSV generation: %s\n", fqName)
				} else {
					if record.NumRows() == chunkLen {
						valueList = append(valueList, record.Column((*statePointer).ColumnMap()[name]))
						parsedColumnMap[fqName] = true
					} else {
						newBuilder := array.NewStringBuilder(pool)
						defer newBuilder.Release()
						for i := int64(0); i < chunkLen; i++ {
							newBuilder.AppendNull()
						}
						valueList = append(valueList, newBuilder.NewArray())
						parsedColumnMap[fqName] = true
					}
					identifierValuesMap[fqName] = valueList
				}
			}
			for _, name := range (*statePointer).MeasurementNames() {
				fqName := (*statePointer).Origin() + "." + strings.Join(strings.Split(name, ".")[1:], ".")
				if parsedColumnMap[fqName] {
					fmt.Printf("Column already parsed during CSV generation at start time %d: %s\n", startTime, fqName)
					continue
				}
				valueList, ok := measurementValuesMap[fqName]
				if !ok {
					fmt.Printf("Measurement column not found during CSV generation: %s\n", fqName)
				} else {
					if record.NumRows() == chunkLen {
						valueList = append(valueList, record.Column((*statePointer).ColumnMap()[name]))
						parsedColumnMap[fqName] = true
					} else {
						newBuilder := array.NewFloat64Builder(pool)
						defer newBuilder.Release()
						for i := int64(0); i < numRows; i++ {
							newBuilder.AppendNull()
						}
						valueList = append(valueList, newBuilder.NewArray())
						parsedColumnMap[fqName] = true
					}
					measurementValuesMap[fqName] = valueList
				}
			}
			for _, name := range (*statePointer).CategoryNames() {
				fqName := (*statePointer).Origin() + "." + strings.Join(strings.Split(name, ".")[1:], ".")
				if parsedColumnMap[fqName] {
					fmt.Printf("Column already parsed during CSV generation at start time %d: %s\n", startTime, fqName)
					continue
				}
				valueMap, ok := categoryOneHotMap[fqName]
				if !ok {
					fmt.Printf("Category column not found during CSV generation: %s\n", fqName)
				} else {
					valueBuilders := make([]*array.Int8Builder, len(valueMap))
					for valueIndex := range valueBuilders {
						valueBuilders[valueIndex] = array.NewInt8Builder(pool)
					}
					if record.NumRows() == chunkLen {
						recordValues := record.Column((*statePointer).ColumnMap()[name]).(*array.String)
						for i := int64(0); i < chunkLen; i++ {
							if recordValues.IsNull(int(i)) {
								for _, builder := range valueBuilders {
									builder.Append(0)
								}
							} else {
								recordValueIndex := valueMap[recordValues.Value(int(i))]
								for valueIndex, builder := range valueBuilders {
									if valueIndex == recordValueIndex {
										builder.Append(1)
									} else {
										builder.Append(0)
									}
								}
							}
						}
						for valueIndex, oneHotName := range categoryOneHotNameMap[fqName] {
							parsedColumnMap[oneHotName] = true
							valueList := categoryValuesMap[oneHotName]
							categoryValuesMap[oneHotName] = append(valueList, valueBuilders[valueIndex].NewArray())
							valueBuilders[valueIndex].Release()
						}
					} else {
						for _, oneHotName := range categoryOneHotNameMap[fqName] {
							newBuilder := array.NewBooleanBuilder(pool)
							defer newBuilder.Release()
							for i := int64(0); i < numRows; i++ {
								newBuilder.Append(false)
							}
							valueList := categoryValuesMap[oneHotName]
							categoryValuesMap[oneHotName] = append(valueList, newBuilder.NewArray())
						}
					}
				}
			}
		}
		// Filling lacking data
		for fqName, valueList := range measurementValuesMap {
			if !parsedColumnMap[fqName] {
				newBuilder := array.NewFloat64Builder(pool)
				defer newBuilder.Release()
				for i := int64(0); i < chunkLen; i++ {
					newBuilder.AppendNull()
				}
				newList := newBuilder.NewArray()
				parsedColumnMap[fqName] = true
				measurementValuesMap[fqName] = append(valueList, newList)
			}
		}
		for fqName, valueList := range identifierValuesMap {
			if !parsedColumnMap[fqName] {
				newBuilder := array.NewStringBuilder(pool)
				defer newBuilder.Release()
				for i := int64(0); i < chunkLen; i++ {
					newBuilder.AppendNull()
				}
				newList := newBuilder.NewArray()
				parsedColumnMap[fqName] = true
				identifierValuesMap[fqName] = append(valueList, newList)
			}
		}
		for _, dataspace := range pod.Dataspaces() {
			for _, category := range dataspace.Categories() {
				for _, fqName := range category.EncodedFieldNames {
					if !parsedColumnMap[fqName] {
						newBuilder := array.NewInt8Builder(pool)
						defer newBuilder.Release()
						for i := int64(0); i < chunkLen; i++ {
							newBuilder.AppendNull()
						}
						newList := newBuilder.NewArray()
						parsedColumnMap[fqName] = true
						valueList := categoryValuesMap[fqName]
						categoryValuesMap[fqName] = append(valueList, newList)
					}
				}
			}
		}
		for i := 0; i < int(chunkLen); i++ {
			if csvTag {
				var rowTags []string
				for _, statePointer := range stateMap[startTime] {
					if (*(*statePointer).Record()).NumRows() == chunkLen {
						tagCol := (*(*statePointer).Record()).Column(int((*(*statePointer).Record()).NumCols() - 1)).(*array.List)
						endOffset := tagCol.Offsets()[i+1]
						tagValues := tagCol.ListValues().(*array.String)
						if tagCol.IsValid(i) {
							for pos := tagCol.Offsets()[i]; pos < endOffset; pos++ {
								rowTags = append(rowTags, tagValues.Value(int(pos)))
							}
						}
					}
				}
				if len(rowTags) == 0 {
					tagBuilder.AppendNull()
				} else {
					tagBuilder.Append(strings.Join(rowTags, " "))
				}
			} else {
				tagListBuilder.Append(true)
				for _, statePointer := range stateMap[startTime] {
					if (*(*statePointer).Record()).NumRows() == chunkLen {
						tagCol := (*(*statePointer).Record()).Column(int((*(*statePointer).Record()).NumCols() - 1)).(*array.List)
						endOffset := tagCol.Offsets()[i+1]
						tagValues := tagCol.ListValues().(*array.String)
						if tagCol.IsValid(i) {
							for pos := tagCol.Offsets()[i]; pos < endOffset; pos++ {
								tagValueBuilder.Append(tagValues.Value(int(pos)))
							}
						}
					}
				}
			}
		}
	}

	var timeCol arrow.Array
	if numRows == 0 {
		builder := array.NewInt64Builder(pool)
		defer builder.Release()
		timeCol = builder.NewArray()
	} else {
		timeCol, _ = array.Concatenate(timeValues, pool)
	}
	cols := []arrow.Array{timeCol}
	for _, timeCategoryName := range pod.timeCategoryNames {
		if numRows == 0 {
			builder := array.NewInt8Builder(pool)
			defer builder.Release()
			cols = append(cols, builder.NewArray())
		} else {
			for _, timeCategory := range pod.timeCategories[timeCategoryName] {
				newCol, err := array.Concatenate(timeValuesMap[timeCategory.FieldName], pool)
				if err != nil {
					log.Fatalf("Error while creating column %s: %q\n", timeCategory.FieldName, err)
				}
				cols = append(cols, newCol)
			}
		}
	}
	for _, idName := range pod.fqIdentifierNames {
		if numRows == 0 {
			builder := array.NewInt8Builder(pool)
			defer builder.Release()
			cols = append(cols, builder.NewArray())
		} else {
			newCol, err := array.Concatenate(identifierValuesMap[idName], pool)
			if err != nil {
				log.Fatalf("Error while creating column %s: %q\n", idName, err)
			}
			cols = append(cols, newCol)
		}
	}
	for _, measurementName := range pod.fqMeasurementNames {
		if numRows == 0 {
			builder := array.NewFloat64Builder(pool)
			defer builder.Release()
			cols = append(cols, builder.NewArray())
		} else {
			newCol, err := array.Concatenate(measurementValuesMap[measurementName], pool)
			if err != nil {
				log.Fatalf("Error while creating column %s: %q\n", measurementName, err)
			}
			cols = append(cols, newCol)
		}
	}
	for _, dataspace := range pod.Dataspaces() {
		for _, category := range dataspace.Categories() {
			for _, fqName := range category.EncodedFieldNames {
				if numRows == 0 {
					builder := array.NewInt8Builder(pool)
					defer builder.Release()
					cols = append(cols, builder.NewArray())
				} else {
					newCol, err := array.Concatenate(categoryValuesMap[fqName], pool)
					if err != nil {
						log.Fatalf("Error while creating column %s: %q\n", fqName, err)
					}
					cols = append(cols, newCol)
				}
			}
		}
	}
	if csvTag {
		cols = append(cols, tagBuilder.NewArray())
	} else {
		cols = append(cols, tagListBuilder.NewArray())
	}
	schema := arrow.NewSchema(fqFields, nil)
	record := array.NewRecord(schema, cols, numRows)

	return record
}

func (pod *Pod) CachedCsv() string {
	record := pod.CachedRecord(true)
	bytebuffer := new(bytes.Buffer)
	writer := csv.NewWriter(bytebuffer, record.Schema(), csv.WithHeader(true), csv.WithComma(','), csv.WithNullWriter(""))
	err := writer.Write(record)
	if err != nil {
		log.Fatalf("Error while writing CSV: %s\n", err)
	}

	return bytebuffer.String()
}

func (pod *Pod) CachedJson() string {
	if len(pod.CachedState()) == 0 {
		return "[]"
	}
	record := pod.CachedRecord(false)
	if record.NumRows() == 0 {
		return "[]"
	}

	builder := strings.Builder{}
	builder.WriteString("[")

	tagCol := record.Column(int(record.NumCols() - 1)).(*array.List)
	tagOffsets := tagCol.Offsets()[1:]
	tagValues := tagCol.ListValues().(*array.String)
	tagPos := 0

	rowIndex := 0
	for {
		builder.WriteString("{")
		builder.WriteString(
			fmt.Sprintf("\"time\":%d", record.Column(0).(*array.Int64).Value(rowIndex)))
		colIndex := 1
		if len(pod.fqIdentifierNames) > 0 {
			var entries []string
			builder.WriteString(",\"identifiers\":{")
			for _, colName := range pod.fqIdentifierNames {
				if !record.Column(colIndex).IsNull(rowIndex) {
					entryValue := record.Column(colIndex).(*array.String).Value(rowIndex)
					if entryValue != "" {
						entries = append(entries, fmt.Sprintf("\"%s\":%s", colName, entryValue))
					}
				}
				colIndex++
			}
			builder.WriteString("}")
			if len(entries) > 0 {
				builder.WriteString(",\"identifiers\":{")
				for i, entry := range entries {
					builder.WriteString(entry)
					if i < len(entries)-1 {
						builder.WriteString(",")
					}
				}
				builder.WriteString("}")
			}
		}
		if len(pod.fqMeasurementNames) > 0 {
			var entries []string
			for _, colName := range pod.fqMeasurementNames {
				if !record.Column(colIndex).IsNull(rowIndex) {
					entryValue := record.Column(colIndex).(*array.Float64).Value(rowIndex)
					if entryValue != 0 {
						entries = append(entries, fmt.Sprintf("\"%s\":%s", colName, strconv.FormatFloat(entryValue, 'f', -1, 64)))
					}
				}
				colIndex++
			}
			if len(entries) > 0 {
				builder.WriteString(",\"measurements\":{")
				for i, entry := range entries {
					builder.WriteString(entry)
					if i < len(entries)-1 {
						builder.WriteString(",")
					}
				}
				builder.WriteString("}")
			}
		}
		if len(pod.fqCategoryNames) > 0 {
			builder.WriteString(",\"categories\":{")
			for catIndex, colName := range pod.fqCategoryNames {
				builder.WriteString(
					fmt.Sprintf("\"%s\":\"%d\"", colName, record.Column(colIndex).(*array.Int8).Value(rowIndex)))
				if catIndex < len(pod.fqCategoryNames)-1 {
					builder.WriteString(",")
				}
				colIndex++
			}
			builder.WriteString("}")
		}
		if tagCol.IsValid(rowIndex) {
			if tagPos < int(tagOffsets[rowIndex]) {
				builder.WriteString(",\"tags\":[")
				for {
					builder.WriteString(fmt.Sprintf("\"%s\"", tagValues.Value(int(tagPos))))
					tagPos++
					if tagPos < int(tagOffsets[rowIndex]) {
						builder.WriteString(",")
					} else {
						break
					}
				}
				builder.WriteString("]")
			}
		}
		rowIndex++
		if rowIndex < int(record.NumRows()) {
			builder.WriteString("},")
		} else {
			builder.WriteString("}")
			break
		}
	}

	builder.WriteString("]")
	return builder.String()
}

func (pod *Pod) Dataspaces() []*dataspace.Dataspace {
	return pod.dataspaces
}

func (pod *Pod) Flights() *map[string]*flights.Flight {
	return &pod.flights
}

func (pod *Pod) Interpretations() *interpretations.InterpretationsStore {
	return pod.interpretations
}

func (pod *Pod) GetDataspace(path string) *dataspace.Dataspace {
	return pod.dataspaceMap[path]
}

func (pod *Pod) GetFlight(flight string) *flights.Flight {
	f, ok := pod.flights[flight]
	if ok {
		return f
	}
	return nil
}

func (pod *Pod) AddFlight(flightId string, flight *flights.Flight) {
	pod.flights[flightId] = flight
}

func (pod *Pod) Actions() map[string]string {
	return pod.actions
}

func (pod *Pod) ActionsArgs() []string {
	actionsArgsMap := make(map[string]bool)
	for _, globalAction := range pod.PodSpec.Actions {
		if globalAction.Do == nil {
			continue
		}
		for argName := range globalAction.Do.Args {
			actionsArgsMap[fmt.Sprintf("args.%s", argName)] = true
		}
	}

	actionsArgs := make([]string, 0, len(actionsArgsMap))
	for arg := range actionsArgsMap {
		actionsArgs = append(actionsArgs, arg)
	}

	return actionsArgs
}

func (pod *Pod) ExternalRewardFuncs() string {
	return pod.externalRewardFuncs
}

func (pod *Pod) Rewards() map[string]string {
	rewards := make(map[string]string)

	if pod.PodSpec.Training == nil || pod.PodSpec.Training.Rewards == nil || pod.PodSpec.Training.Rewards == "uniform" {
		for actionName := range pod.Actions() {
			rewards[actionName] = "reward = 1"
		}
		return rewards
	}

	rewardSpecs, err := pod.loadRewardSpecs()
	if err != nil {
		for actionName := range pod.Actions() {
			rewards[actionName] = "reward = 1"
		}
		return rewards
	}

	for _, reward := range rewardSpecs {
		rewards[reward.Reward] = reward.With
	}

	return rewards
}

func (pod *Pod) Measurements() map[string]*dataspace.MeasurementInfo {
	return pod.measurements
}

// Returns the list of fully-qualified identifier names
func (pod *Pod) IdentifierNames() []string {
	return pod.fqIdentifierNames
}

// Returns the list of fully-qualified measurement names
func (pod *Pod) MeasurementNames() []string {
	return pod.fqMeasurementNames
}

// Returns the list of fully-qualified category names
func (pod *Pod) CategoryNames() []string {
	return pod.fqCategoryNames
}

// Returns the global list of tag values
func (pod *Pod) Tags() []string {
	return pod.tags
}

func (pod *Pod) GetLogDir() (string, error) {
	if pod.logDir == "" {
		logDir, err := tempdir.CreateTempDir(pod.Name)
		if err != nil {
			return "", err
		}
		pod.logDir = logDir
	}
	return pod.logDir, nil
}

func (pod *Pod) ValidateForTraining() error {
	// Consider using something like https://github.com/go-playground/validator in the future
	if pod.Granularity() > pod.Interval() {
		return errors.New("granularity must be less than or equal to interval")
	}

	if pod.Interval() > pod.Period() {
		return errors.New("interval must be less than or equal to period")
	}

	if pod.PodSpec.Dataspaces == nil || len(pod.PodSpec.Dataspaces) < 1 {
		return errors.New("at least one dataspace is required for training")
	}

	for _, ds := range pod.PodSpec.Dataspaces {
		valid := validator.ValidateDataspaceName(ds.From)
		if !valid {
			return fmt.Errorf("invalid dataspace \"from\": '%s' should only contain A-Za-z0-9_", ds.From)
		}
		valid = validator.ValidateDataspaceName(ds.Name)
		if !valid {
			return fmt.Errorf("invalid dataspace \"name\": '%s' should only contain A-Za-z0-9_", ds.Name)
		}

		for _, f := range ds.Measurements {
			switch f.Fill {
			case "":
			case "previous":
			case "none":
			default:
				return fmt.Errorf("invalid measurement fill '%s': choose one of ['previous', 'none']", f.Fill)
			}
		}
	}

	actions := pod.Actions()

	if len(actions) == 0 {
		return errors.New("at least one action is required for training")
	}

	// Check for args.<arg name>
	rewards := pod.Rewards()

	for actionName, action := range actions {
		numErrors := 0
		matches := validator.GetArgsRegex().FindStringSubmatch(action)
		errorLines := strings.Builder{}
		for i, match := range matches {
			if i == 0 {
				continue
			}
			numErrors++
			errorLines.WriteString(fmt.Sprintf("action '%s' references undefined 'args.%s'\n", actionName, strings.TrimSpace(match)))
		}

		// Each action must have a reward. TODO: Validate the reward is a valid expression.
		if reward, ok := rewards[actionName]; !ok {
			numErrors++
			errorLines.WriteString(fmt.Sprintf("reward for action '%s' is invalid '%s'\n", actionName, reward))
		}

		if numErrors > 0 {
			return errors.New(errorLines.String())
		}
	}

	return nil
}

func (pod *Pod) AddLocalState(newState ...*state.State) {
	pod.podLocalStateMutex.Lock()
	defer pod.podLocalStateMutex.Unlock()

	pod.podLocalState = append(pod.podLocalState, newState...)
}

func (pod *Pod) State() []*state.State {
	return pod.podLocalState
}

func (pod *Pod) LearningAlgorithm() string {
	return pod.podParams.LearningAlgorithm
}

func (pod *Pod) InitDataConnectors(handler state.StateHandler) error {
	pod.podLocalStateMutex.Lock()
	defer pod.podLocalStateMutex.Unlock()

	pod.podLocalStateHandlers = append(pod.podLocalStateHandlers, handler)

	errGroup, _ := errgroup.WithContext(context.Background())

	for _, ds := range pod.Dataspaces() {
		dsp := ds
		errGroup.Go(func() error {
			dsp.RegisterStateHandler(handler)
			return dsp.InitDataConnector(pod.podParams.Epoch, pod.podParams.Period, pod.podParams.Interval)
		})
	}

	return errGroup.Wait()
}

func unmarshalPod(podPath string) (*Pod, error) {
	podBytes, err := util.ReplaceEnvVariablesFromPath(podPath, constants.SpiceEnvVarPrefix)
	if err != nil {
		return nil, err
	}

	v := viper.New()
	v.SetConfigType("yaml")

	err = v.ReadConfig(bytes.NewBuffer(podBytes))
	if err != nil {
		return nil, err
	}

	var podSpec *spec.PodSpec

	err = v.Unmarshal(&podSpec)
	if err != nil {
		return nil, err
	}

	pod := &Pod{
		PodSpec:            *podSpec,
		viper:              v,
		podLocalStateMutex: sync.RWMutex{},
	}

	return pod, nil
}

func loadPod(podPath string, hash string) (*Pod, error) {
	pod, err := unmarshalPod(podPath)
	if err != nil {
		return nil, err
	}

	err = pod.loadParams()
	if err != nil {
		return nil, fmt.Errorf("error loading pod params: %s", err.Error())
	}

	pod.manifestPath = podPath
	pod.hash = hash
	if pod.Name == "" {
		pod.Name = strings.TrimSuffix(filepath.Base(podPath), filepath.Ext(podPath))
	}

	if pod.Time != nil && len(pod.Time.Categories) > 0 {
		pod.timeCategories = spice_time.GenerateTimeCategoryFields(pod.Time.Categories...)
		names := make([]string, len(pod.timeCategories))
		for name := range pod.timeCategories {
			names = append(names, name)
		}
		sort.Strings(names)
		pod.timeCategoryNames = names
	}

	pod.flights = make(map[string]*flights.Flight)

	var fqIdentifierNames []string
	var fqMeasurementNames []string
	var fqCategoryNames []string
	var tags []string

	tagsMap := make(map[string]bool)
	measurements := make(map[string]*dataspace.MeasurementInfo)
	dataspaceMap := make(map[string]*dataspace.Dataspace, len(pod.PodSpec.Dataspaces))

	for _, dsSpec := range pod.PodSpec.Dataspaces {
		ds, err := dataspace.NewDataspace(dsSpec)
		if err != nil {
			return nil, err
		}
		pod.dataspaces = append(pod.dataspaces, ds)
		dataspaceMap[ds.Path()] = ds

		for _, identifier := range ds.Identifiers() {
			fqIdentifierNames = append(fqIdentifierNames, identifier.FqName)
		}

		for fqMeasurementName, measurement := range ds.Measurements() {
			fqMeasurementNames = append(fqMeasurementNames, fqMeasurementName)
			measurements[fqMeasurementName] = measurement
		}

		for _, category := range ds.Categories() {
			fqCategoryNames = append(fqCategoryNames, category.FqName)
		}

		for _, dsTag := range ds.Tags() {
			if _, ok := tagsMap[dsTag]; !ok {
				tagsMap[dsTag] = true
				tags = append(tags, dsTag)
			}
		}
	}

	pod.dataspaceMap = dataspaceMap

	pod.actions = pod.getActions()

	sort.Strings(fqIdentifierNames)
	pod.fqIdentifierNames = fqIdentifierNames

	sort.Strings(fqMeasurementNames)
	pod.fqMeasurementNames = fqMeasurementNames
	pod.measurements = measurements

	sort.Strings(fqCategoryNames)
	pod.fqCategoryNames = fqCategoryNames

	sort.Strings(tags)
	pod.tags = tags

	pod.interpretations = interpretations.NewInterpretationsStore(pod.Epoch(), pod.Period(), pod.Granularity())

	if pod.Training != nil && pod.Training.RewardFuncs != "" {
		if !strings.HasSuffix(pod.Training.RewardFuncs, ".py") {
			return nil, errors.New("external reward functions must be defined in a single Python file - see https://docs.spiceai.org/concepts/rewards/")
		}

		rewardFuncBytes, err := os.ReadFile(pod.Training.RewardFuncs)
		if err != nil {
			return nil, err
		}

		pod.externalRewardFuncs = string(rewardFuncBytes)
	}

	return pod, err
}

func (pod *Pod) loadRewardSpecs() ([]spec.RewardSpec, error) {
	var rewards []spec.RewardSpec
	err := pod.viper.UnmarshalKey("training.rewards", &rewards)
	if err != nil {
		return nil, err
	}
	return rewards, nil
}

func (pod *Pod) IsSame(otherPod *Pod) bool {
	return otherPod != nil && pod.manifestPath == otherPod.manifestPath && pod.Hash() == otherPod.Hash()
}

func (pod *Pod) loadParams() error {
	podParams := NewPodParams()

	if pod.PodSpec.Params != nil {
		str, ok := pod.PodSpec.Params["epoch_time"]
		if ok {
			intVal, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				return err
			}
			podParams.Epoch = time.Unix(intVal, 0).UTC()
		}

		str, ok = pod.PodSpec.Params["period"]
		if ok {
			val, err := time.ParseDuration(str)
			if err != nil {
				return err
			}
			podParams.Period = val
		}

		str, ok = pod.PodSpec.Params["interval"]
		if ok {
			val, err := time.ParseDuration(str)
			if err != nil {
				return err
			}
			podParams.Interval = val
		}

		str, ok = pod.PodSpec.Params["granularity"]
		if ok {
			val, err := time.ParseDuration(str)
			if err != nil {
				return err
			}
			podParams.Granularity = val
		}

		str, ok = pod.PodSpec.Params["interpolation"]
		if ok {
			val, err := strconv.ParseBool(str)
			if err != nil {
				return err
			}
			podParams.Interpolation = val
		}
	}

	pod.podParams = podParams

	return nil
}

func (pod *Pod) getActions() map[string]string {
	allDataSourceActions := make(map[string]string)
	var dataSourcePrefixes []string
	for _, ds := range pod.Dataspaces() {
		for fqActionName, fqAction := range ds.Actions() {
			allDataSourceActions[fqActionName] = fqAction
			dataSourcePrefixes = append(dataSourcePrefixes, fmt.Sprintf("%s.%s", ds.DataspaceSpec.From, ds.DataspaceSpec.Name))
		}
	}

	globalActions := make(map[string]string)
	actions := make(map[string]string)

	for _, globalAction := range pod.PodSpec.Actions {
		if globalAction.Do == nil {
			actions[globalAction.Name] = ""
			continue
		}

		globalActions[globalAction.Name] = globalAction.Do.Name

		dsAction, ok := allDataSourceActions[globalAction.Do.Name]
		if !ok {
			actions[globalAction.Name] = globalAction.Do.Name
			continue
		}

		for argName, argValue := range globalAction.Do.Args {
			action := strings.ReplaceAll(dsAction, fmt.Sprintf("args.%s", argName), argValue)
			actions[globalAction.Name] = action
		}
	}

	// Hoist any dataspace actions to global scope if they don't exist
	for dsActionName, dsAction := range allDataSourceActions {
		dsActionExistsGlobally := false
		for globalActionName := range actions {
			if existingDsActionName, ok := globalActions[globalActionName]; ok {
				if dsActionName == existingDsActionName {
					dsActionExistsGlobally = true
					break
				}
			}
		}
		if !dsActionExistsGlobally {
			for _, prefix := range dataSourcePrefixes {
				if strings.HasPrefix(dsActionName, prefix) {
					actionName := strings.TrimPrefix(dsActionName, prefix+".")
					actions[actionName] = dsAction
					break
				}
			}
		}
	}
	return actions
}
