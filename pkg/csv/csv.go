package csv

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/spiceai/spice/pkg/observations"
	"github.com/spiceai/spice/pkg/state"
	"github.com/spiceai/spice/pkg/util"
)

// Processes CSV into Observations
func ProcessCsv(input io.Reader) ([]observations.Observation, error) {
	headers, lines, err := getCsvHeaderAndLines(input)
	if err != nil {
		return nil, fmt.Errorf("failed to process csv: %s", err)
	}

	var newObservations []observations.Observation
	for line, record := range lines {
		ts, err := util.ParseTime(record[0])
		if err != nil {
			log.Printf("ignoring invalid line %d - %v: %v", line+1, record, err)
			continue
		}

		data := make(map[string]float64)

		for col := 1; col < len(record); col++ {
			field := record[col]
			val, err := strconv.ParseFloat(field, 64)
			if err != nil {
				log.Printf("ignoring invalid field %d - %v: %v", line+1, field, err)
				continue
			}
			data[headers[col]] = val
		}

		observation := observations.Observation{
			Time: ts,
			Data: data,
		}

		newObservations = append(newObservations, observation)
	}

	return newObservations, nil
}

// Processes CSV into State by field path
// CSV headers are expected to be fully-qualified field names
func ProcessCsvByPath(input io.Reader, validFields *[]string) ([]*state.State, error) {
	headers, lines, err := getCsvHeaderAndLines(input)
	if err != nil {
		return nil, fmt.Errorf("failed to process csv: %s", err)
	}

	if validFields != nil {
		for i := 1; i < len(headers); i++ {
			header := headers[i]
			fields := *validFields
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
		pathToFieldNames[path] = append(pathToFieldNames[path], fieldName)
	}

	// TODO: Verbose log
	// log.Printf("Read headers of %v", headers)

	numDataFields := len(headers) - 1

	for line, record := range lines {
		ts, err := util.ParseTime(record[0])
		if err != nil {
			log.Printf("ignoring invalid line %d - %v: %v", line+1, record, err)
			continue
		}

		lineData := make(map[string]map[string]float64, numDataFields)

		for col := 1; col < len(record); col++ {
			field := record[col]

			if field == "" {
				continue
			}

			val, err := strconv.ParseFloat(field, 64)
			if err != nil {
				log.Printf("ignoring invalid field %d - %v: %v", line+1, field, err)
				continue
			}

			fieldCol := col - 1
			fieldName := columnToFieldName[fieldCol]

			path := columnToPath[fieldCol]
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
				Time: ts,
				Data: data,
			}
			obs := pathToObservations[path]
			pathToObservations[path] = append(obs, *observation)
		}
	}

	result := make([]*state.State, len(pathToObservations))

	i := 0
	for path, obs := range pathToObservations {
		fieldNames := pathToFieldNames[path]
		result[i] = state.NewState(path, fieldNames, obs)
		i++
	}

	return result, nil
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
