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
	fqIdentifierNames    []string
	identifiersNamesMap  map[string]string
	measurementsNames    []string
	fqMeasurementsNames  []string
	measurementsNamesMap map[string]string
	categoryNamesMap     map[string]string
	tags                 []string
	observations         []observations.Observation
	observationsMutex    sync.RWMutex
}

type StateHandler func(state *State, metadata map[string]string) error

func NewState(path string, identifierNames []string, measurementsNames []string, categoryNames []string, tags []string, observations []observations.Observation) *State {
	fqIdentifierNames, identifiersNamesMap := getFieldNames(path, identifierNames)
	sort.Strings(fqIdentifierNames)

	fqMeasurementsNames, measurementsNamesMap := getFieldNames(path, measurementsNames)
	sort.Strings(fqMeasurementsNames)

	_, categoryNamesMap := getFieldNames(path, categoryNames)

	return &State{
		Time:                 time.Now(),
		TimeSentToAIEngine:   time.Time{},
		path:                 path,
		fqIdentifierNames:    fqIdentifierNames,
		identifiersNamesMap:  identifiersNamesMap,
		measurementsNames:    measurementsNames,
		fqMeasurementsNames:  fqMeasurementsNames,
		measurementsNamesMap: measurementsNamesMap,
		categoryNamesMap:     categoryNamesMap,
		tags:                 tags,
		observations:         observations,
	}
}

type csvLineData struct {
	identifiers  map[string]string
	measurements map[string]float64
	categories   map[string]string
}

type csvDataspaceData struct {
	observations     []observations.Observation
	identifierNames  []string
	measurementNames []string
	categoryNames    []string
}

// Processes into State by field path
// CSV headers are expected to be fully-qualified field names
func GetStateFromCsv(validIdentifierNames []string, validMeasurementNames []string, validCategoryNames []string, data []byte) ([]*State, error) {
	reader := bytes.NewReader(data)
	if reader == nil {
		return nil, nil
	}

	headers, lines, err := getCsvHeaderAndLines(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to process csv: %s", err)
	}

	dsPathsMap, colToDsPath, colToIdentifierName, colToMeasurementName, colToCategoryName, colToTagsName, err := processCsvHeaders(headers, validIdentifierNames, validMeasurementNames, validCategoryNames)
	if err != nil {
		return nil, fmt.Errorf("failed to process csv headers: %s", err)
	}

	// Group data into dataspace paths
	dsPathToData := getDsPathToDataMap(len(dsPathsMap), colToDsPath, colToIdentifierName, colToMeasurementName, colToCategoryName)

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
			fieldValue := record[col]

			if fieldValue == "" {
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

			// Identifiers
			identifierName := colToIdentifierName[fieldCol]
			if identifierName != "" {
				if lineData.identifiers == nil {
					lineData.identifiers = make(map[string]string, len(validIdentifierNames))
				}
				lineData.identifiers[identifierName] = fieldValue
				continue
			}

			// Measurements
			measurementName := colToMeasurementName[fieldCol]
			if measurementName != "" {
				val, err := strconv.ParseFloat(fieldValue, 64)
				if err != nil {
					log.Printf("ignoring invalid measurement field %d - %v: %v", line+1, fieldValue, err)
					continue
				}
				if lineData.measurements == nil {
					lineData.measurements = make(map[string]float64, len(validMeasurementNames))
				}
				lineData.measurements[measurementName] = val
				continue
			}

			// Categories
			categoryName := colToCategoryName[fieldCol]
			if categoryName != "" {
				if lineData.categories == nil {
					lineData.categories = make(map[string]string, len(validCategoryNames))
				}
				lineData.categories[categoryName] = fieldValue
				continue
			}

			// Tags
			if colToTagsName[fieldCol] != "" {
				tags := strings.Split(fieldValue, " ")
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
		}

		for path := range dsPathsMap {
			dsLineData, ok := dsLineData[path]
			if !ok {
				continue
			}
			observation := observations.Observation{
				Time:         ts.Unix(),
				Identifiers:  dsLineData.identifiers,
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

		sort.Strings(dsData.identifierNames)
		sort.Strings(dsData.measurementNames)
		sort.Strings(dsData.categoryNames)
		sort.Strings(tags)

		result = append(result, NewState(path, dsData.identifierNames, dsData.measurementNames, dsData.categoryNames, tags, dsData.observations))
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
	return s.fqMeasurementsNames
}

// Returns map of identifiers names to fully-qualified identifiers names
func (s *State) IdentifiersNamesMap() map[string]string {
	return s.identifiersNamesMap
}

// Returns map of measurement names to fully-qualified meausurement names
func (s *State) MeasurementsNamesMap() map[string]string {
	return s.measurementsNamesMap
}

// Returns map of category names to fully-qualified category names
func (s *State) CategoryNamesMap() map[string]string {
	return s.categoryNamesMap
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
func processCsvHeaders(headers []string, validIdentifierNames []string, validMeasurementNames []string, validCategoryNames []string) (map[string]bool, []string, []string, []string, []string, []string, error) {
	numDataFields := len(headers) - 1
	dataspacePathsMap := make(map[string]bool)
	columnToDataspacePath := make([]string, numDataFields)
	columnToIdentifierName := make([]string, numDataFields)
	columnToMeasurementName := make([]string, numDataFields)
	columnToCategoryName := make([]string, numDataFields)
	columnToTagsName := make([]string, numDataFields)
	for i := 0; i < numDataFields; i++ {
		header := headers[i+1]
		found := false
		dotIndex := strings.LastIndex(header, ".")
		if dotIndex == -1 {
			return nil, nil, nil, nil, nil, nil, fmt.Errorf("header '%s' expected to be full-qualified", header)
		}
		for _, validIdentifierName := range validIdentifierNames {
			if validIdentifierName == header {
				found = true
				columnToIdentifierName[i] = validIdentifierName[dotIndex+1:]
				break
			}
		}
		if !found {
			for _, validMeasurementName := range validMeasurementNames {
				if validMeasurementName == header {
					found = true
					columnToMeasurementName[i] = validMeasurementName[dotIndex+1:]
					break
				}
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

	return dataspacePathsMap, columnToDataspacePath, columnToIdentifierName, columnToMeasurementName, columnToCategoryName, columnToTagsName, nil
}

// Returns measurements, and categories grouped by datasource path
func getDsPathToDataMap(numDataspaces int, colToDsPath []string, colToIdentifierName []string, colToMeasurementName []string, colToCategoryName []string) map[string]*csvDataspaceData {
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
		identifierName := colToIdentifierName[col]
		if identifierName != "" {
			dsData.identifierNames = append(dsData.identifierNames, identifierName)
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
