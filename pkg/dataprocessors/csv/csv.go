package csv

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/spiceai/spiceai/pkg/loggers"
	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/spiceai/spiceai/pkg/state"
	"github.com/spiceai/spiceai/pkg/util"
	"go.uber.org/zap"
)

var (
	zaplog *zap.Logger = loggers.ZapLogger()
)

const (
	CsvProcessorName string = "csv"
)

type CsvProcessor struct {
	data      []byte
	dataMutex sync.RWMutex
	dataHash  []byte
}

func NewCsvProcessor() *CsvProcessor {
	return &CsvProcessor{}
}

func (p *CsvProcessor) Init(params map[string]string) error {
	return nil
}

func (p *CsvProcessor) OnData(data []byte) ([]byte, error) {
	p.dataMutex.Lock()
	defer p.dataMutex.Unlock()

	newDataHash, err := util.ComputeNewHash(p.data, p.dataHash, data)
	if err != nil {
		return nil, fmt.Errorf("error computing new data hash in csv processor: %w", err)
	}

	if newDataHash != nil {
		// Only update data if new
		p.data = data
		p.dataHash = newDataHash
	}

	return data, nil
}

func (p *CsvProcessor) GetObservations() ([]observations.Observation, error) {
	p.dataMutex.Lock()
	defer p.dataMutex.Unlock()

	reader, err := p.getDataReader()
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, nil
	}

	newObservations, err := p.getObservations(reader)
	if err != nil {
		return nil, err
	}

	p.data = nil
	return newObservations, nil
}

func (p *CsvProcessor) getObservations(reader io.Reader) ([]observations.Observation, error) {
	headers, lines, err := getCsvHeaderAndLines(reader)
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

// Processes into State by field path
// CSV headers are expected to be fully-qualified field names
func (p *CsvProcessor) GetState(validFields *[]string) ([]*state.State, error) {
	p.dataMutex.Lock()
	defer p.dataMutex.Unlock()

	reader, err := p.getDataReader()
	if err != nil {
		return nil, err
	}
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

	zaplog.Sugar().Debugf("Read headers of %v", headers)

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

	p.data = nil
	return result, nil
}

func (p *CsvProcessor) getDataReader() (io.Reader, error) {
	if p.data == nil {
		return nil, nil
	}

	reader := bytes.NewReader(p.data)
	return reader, nil
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
