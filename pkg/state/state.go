package state

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spiceai/spiceai/pkg/observations"
	spice_time "github.com/spiceai/spiceai/pkg/time"
)

type State struct {
	Time                 time.Time
	TimeSentToAIEngine   time.Time
	path                 string
	measurementsNames    []string
	fqMeasurementsNames  []string
	measurementsNamesMap map[string]string
	categoryNames        []string
	fqCategoryNames      []string
	categoryNamesMap     map[string]string
	tags                 []string
	observations         []observations.Observation
	observationsMutex    sync.RWMutex
}

type StateHandler func(state *State, metadata map[string]string) error

func NewState(path string, measurementsNames []string, tags []string, observations []observations.Observation) *State {
	fqMeasurementsNames, measurementsNamesMap := getFieldNames(path, measurementsNames)

	return &State{
		Time:                 time.Now(),
		TimeSentToAIEngine:   time.Time{},
		path:                 path,
		measurementsNames:    measurementsNames,
		fqMeasurementsNames:  fqMeasurementsNames,
		measurementsNamesMap: measurementsNamesMap,
		tags:                 tags,
		observations:         observations,
	}
}

type csvLineData struct {
	measurements map[string]float64
	categories   map[string]string
}

type csvDataspaceData struct {
	observations     []observations.Observation
	measurementNames []string
	categoryNames    []string
}

// Processes into State by field path
// CSV headers are expected to be fully-qualified field names
func GetStateFromCsv(validMeasurementNames []string, validCategoryNames []string, data []byte) ([]*State, error) {
	reader := bytes.NewReader(data)
	if reader == nil {
		return nil, nil
	}

	headers, lines, err := getCsvHeaderAndLines(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to process csv: %s", err)
	}

	dsPathsMap, colToDsPath, colToMeasurementName, colToCategoryName, colToTagsName, err := processCsvHeaders(headers, validMeasurementNames, validCategoryNames)
	if err != nil {
		return nil, fmt.Errorf("failed to process csv headers: %s", err)
	}

	// Group data into dataspace paths
	dsPathToData := getDsPathToDataMap(len(dsPathsMap), colToDsPath, colToMeasurementName, colToCategoryName)

	// Map from path -> set of detected tags on that path
	dataspacePathToTagsMap := make(map[string]map[string]bool)

	for line, record := range lines {
		ts, err := spice_time.ParseTime(record[0], "")
		if err != nil {
			log.Printf("ignoring invalid line %d - %v: %v", line+1, record, err)
			continue
		}

		dsLineData := make(map[string]*csvLineData, len(dsPathsMap))

		for col := 1; col < len(record); col++ {
			field := record[col]

			if field == "" {
				continue
			}

			fieldCol := col - 1
			path := colToDsPath[fieldCol]
			if path == "" {
				continue
			}

			lineData, ok := dsLineData[path]
			if !ok {
				lineData = &csvLineData{}
				dsLineData[path] = lineData
			}

			if colToTagsName[fieldCol] != "" {
				tags := strings.Split(field, " ")
				for _, tagVal := range tags {
					if tagVal == "" {
						continue
					}
					if _, ok := dataspacePathToTagsMap[path]; !ok {
						dataspacePathToTagsMap[path] = make(map[string]bool)
					}
					dataspacePathToTagsMap[path][tagVal] = true
				}
				continue
			}

			measurementName := colToMeasurementName[fieldCol]
			if measurementName != "" {
				val, err := strconv.ParseFloat(field, 64)
				if err != nil {
					log.Printf("ignoring invalid measurement field %d - %v: %v", line+1, field, err)
					continue
				}
				if lineData.measurements == nil {
					lineData.measurements = make(map[string]float64, len(validMeasurementNames))
				}
				lineData.measurements[measurementName] = val
				continue
			}

			categoryName := colToCategoryName[fieldCol]
			if categoryName != "" {
				if lineData.categories == nil {
					lineData.categories = make(map[string]string, len(validCategoryNames))
				}
				lineData.categories[categoryName] = field
				continue
			}
		}

		for path := range dsPathsMap {
			dsLineData, ok := dsLineData[path]
			if !ok {
				continue
			}
			observation := observations.Observation{
				Time:         ts.Unix(),
				Measurements: dsLineData.measurements,
				Categories:   dsLineData.categories,
			}
			dsData := dsPathToData[path]
			dsData.observations = append(dsData.observations, observation)
		}
	}

	result := make([]*State, 0, len(dsPathToData))

	for path, dsData := range dsPathToData {
		if len(dsData.observations) == 0 {
			continue
		}

		var tags []string
		for tagVal := range dataspacePathToTagsMap[path] {
			tags = append(tags, tagVal)
		}

		sort.Strings(dsData.measurementNames)
		sort.Strings(dsData.categoryNames)
		sort.Strings(tags)

		result = append(result, NewState(path, dsData.measurementNames, tags, dsData.observations))
	}

	return result, nil
}

func (s *State) Path() string {
	return s.path
}

func (s *State) MeasurementsNames() []string {
	return s.measurementsNames
}

func (s *State) FqMeasurementsNames() []string {
	sort.Strings(s.fqMeasurementsNames)
	return s.fqMeasurementsNames
}

// Returns map of measurement names to fully-qualified meausurement names
func (s *State) MeasurementsNamesMap() map[string]string {
	return s.measurementsNamesMap
}

func (s *State) Observations() []observations.Observation {
	return s.observations
}

func (s *State) Tags() []string {
	return s.tags
}

func (s *State) Sent() {
	s.TimeSentToAIEngine = time.Now()
}

func (s *State) AddData(newObservations ...observations.Observation) {
	s.observationsMutex.Lock()
	defer s.observationsMutex.Unlock()

	s.observations = append(s.observations, newObservations...)
}

func getCsvHeaderAndLines(input io.Reader) ([]string, [][]string, error) {
	reader := csv.NewReader(input)
	headers, err := reader.Read()
	if err != nil {
		return nil, nil, errors.New("failed to read header")
	}

	lines, err := reader.ReadAll()
	if err != nil {
		return nil, nil, errors.New("failed to read lines")
	}

	if len(headers) <= 1 || len(lines) == 0 {
		return nil, nil, errors.New("no data")
	}

	// Temporary restriction until mapped fields are supported
	if headers[0] != "time" {
		return nil, nil, errors.New("first column must be 'time'")
	}

	return headers, lines, nil
}

// Process CSV headers. Expected to be fully-qualified.
func processCsvHeaders(headers []string, validMeasurementNames []string, validCategoryNames []string) (map[string]bool, []string, []string, []string, []string, error) {
	numDataFields := len(headers) - 1
	dataspacePathsMap := make(map[string]bool, 0)
	columnToDataspacePath := make([]string, numDataFields)
	columnToMeasurementName := make([]string, numDataFields)
	columnToCategoryName := make([]string, numDataFields)
	columnToTagsName := make([]string, numDataFields)
	for i := 0; i < numDataFields; i++ {
		header := headers[i+1]
		found := false
		dotIndex := strings.LastIndex(header, ".")
		if dotIndex == -1 {
			return nil, nil, nil, nil, nil, fmt.Errorf("header '%s' expected to be full-qualified", header)
		}
		for _, validMeasurementName := range validMeasurementNames {
			if validMeasurementName == header {
				found = true
				columnToMeasurementName[i] = validMeasurementName[dotIndex+1:]
				break
			}
		}
		if !found {
			for _, validCategoryName := range validCategoryNames {
				if validCategoryName == header {
					found = true
					columnToCategoryName[i] = validCategoryName[dotIndex+1:]
					break
				}
			}
		}
		if !found && strings.HasSuffix(header, "._tags") {
			columnToTagsName[i] = header
			found = true
		}
		if !found {
			// Ignore unknown columns
			continue
		}
		dsPath := header[:dotIndex]
		if _, ok := dataspacePathsMap[dsPath]; !ok {
			dataspacePathsMap[dsPath] = true
		}
		columnToDataspacePath[i] = dsPath
	}

	return dataspacePathsMap, columnToDataspacePath, columnToMeasurementName, columnToCategoryName, columnToTagsName, nil
}

// Returns measurements, and categories grouped by datasource path
func getDsPathToDataMap(numDataspaces int, colToDsPath []string, colToMeasurementName []string, colToCategoryName []string) map[string]*csvDataspaceData {
	dsPathToData := make(map[string]*csvDataspaceData, numDataspaces)

	for col, path := range colToDsPath {
		if path == "" {
			continue
		}
		dsData, ok := dsPathToData[path]
		if !ok {
			dsData = &csvDataspaceData{}
			dsPathToData[path] = dsData
		}
		measurementName := colToMeasurementName[col]
		if measurementName != "" {
			dsData.measurementNames = append(dsData.measurementNames, measurementName)
		}
		categoryName := colToCategoryName[col]
		if categoryName != "" {
			dsData.categoryNames = append(dsData.categoryNames, categoryName)
		}
	}

	return dsPathToData
}

// Gets the list of fully-qualified field names, and a map of names to fq names
func getFieldNames(path string, fieldNames []string) ([]string, map[string]string) {
	fqNames := make([]string, len(fieldNames))
	namesMap := make(map[string]string, len(fieldNames))

	for i, name := range fieldNames {
		fqName := path + "." + name
		fqNames[i] = fqName
		namesMap[name] = fqName
	}

	return fqNames, namesMap
}
