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
	Time               time.Time
	TimeSentToAIEngine time.Time
	path               string
	fieldNames         []string
	fields             []string
	tags               []string
	observations       []observations.Observation
	observationsMutex  sync.RWMutex
}

type StateHandler func(state *State, metadata map[string]string) error

func NewState(path string, fieldNames []string, tags []string, observations []observations.Observation) *State {
	fields := make([]string, len(fieldNames))
	for i, name := range fieldNames {
		fields[i] = path + "." + name
	}

	return &State{
		Time:               time.Now(),
		TimeSentToAIEngine: time.Time{},
		path:               path,
		fieldNames:         fieldNames,
		fields:             fields,
		tags:               tags,
		observations:       observations,
	}
}

// Processes into State by field path
// CSV headers are expected to be fully-qualified field names
func GetStateFromCsv(validFields []string, data []byte) ([]*State, error) {
	reader := bytes.NewReader(data)
	if reader == nil {
		return nil, nil
	}

	headers, lines, err := getCsvHeaderAndLines(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to process csv: %s", err)
	}

	if validFields != nil {
		for i := 1; i < len(headers); i++ {
			header := headers[i]
			fields := validFields
			found := false
			for _, validField := range fields {
				if validField == header {
					found = true
					break
				}
			}

			if !found {
				return nil, fmt.Errorf("unknown field '%s'", header)
			}
		}
	}

	columnToPath, columnToFieldName, err := getColumnMappings(headers)
	if err != nil {
		return nil, fmt.Errorf("failed to process csv: %s", err)
	}

	pathToObservations := make(map[string][]observations.Observation)
	pathToFieldNames := make(map[string][]string)

	for col, path := range columnToPath {
		_, ok := pathToObservations[path]
		if !ok {
			pathToObservations[path] = make([]observations.Observation, 0)
			pathToFieldNames[path] = make([]string, 0)
		}
		fieldName := columnToFieldName[col]

		if fieldName == "_tags" {
			continue
		}

		pathToFieldNames[path] = append(pathToFieldNames[path], fieldName)
	}

	numDataFields := len(headers) - 1

	// Map from path -> set of detected tags on that path
	allTagData := make(map[string]map[string]bool)

	for line, record := range lines {
		ts, err := spice_time.ParseTime(record[0], "")
		if err != nil {
			log.Printf("ignoring invalid line %d - %v: %v", line+1, record, err)
			continue
		}

		lineData := make(map[string]map[string]float64, numDataFields)
		tagData := make(map[string][]string)

		for col := 1; col < len(record); col++ {
			field := record[col]

			if field == "" {
				continue
			}

			fieldCol := col - 1
			path := columnToPath[fieldCol]
			fieldName := columnToFieldName[fieldCol]

			if fieldName == "_tags" {
				tagData[path] = strings.Split(field, " ")

				for _, tagVal := range tagData[path] {
					if _, ok := allTagData[path]; !ok {
						allTagData[path] = make(map[string]bool)
					}
					allTagData[path][tagVal] = true
				}
				continue
			}

			val, err := strconv.ParseFloat(field, 64)
			if err != nil {
				log.Printf("ignoring invalid field %d - %v: %v", line+1, field, err)
				continue
			}

			data := lineData[path]
			if data == nil {
				data = make(map[string]float64)
				lineData[path] = data
			}

			data[fieldName] = val
		}

		if len(lineData) == 0 {
			continue
		}

		for path, data := range lineData {
			observation := &observations.Observation{
				Time: ts.Unix(),
				Data: data,
				Tags: tagData[path],
			}
			obs := pathToObservations[path]
			pathToObservations[path] = append(obs, *observation)
		}
	}

	result := make([]*State, len(pathToObservations))

	i := 0
	for path, obs := range pathToObservations {
		tags := make([]string, 0)
		for tagVal := range allTagData[path] {
			tags = append(tags, tagVal)
		}
		sort.Strings(tags)

		fieldNames := pathToFieldNames[path]
		result[i] = NewState(path, fieldNames, tags, obs)
		i++
	}

	return result, nil
}

func (s *State) Path() string {
	return s.path
}

func (s *State) FieldNames() []string {
	return s.fieldNames
}

func (s *State) Fields() []string {
	return s.fields
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

// Returns mapping of column index to path and field name
func getColumnMappings(headers []string) ([]string, []string, error) {
	numDataFields := len(headers) - 1

	columnToPath := make([]string, numDataFields)
	columnToFieldName := make([]string, numDataFields)

	for i := 1; i < len(headers); i++ {
		header := headers[i]
		dotIndex := strings.LastIndex(header, ".")
		if dotIndex == -1 {
			return nil, nil, fmt.Errorf("header '%s' expected to be full-qualified", header)
		}
		columnToPath[i-1] = header[:dotIndex]
		columnToFieldName[i-1] = header[dotIndex+1:]
	}

	return columnToPath, columnToFieldName, nil
}
