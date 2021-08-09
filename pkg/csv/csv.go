package csv

import (
	"encoding/csv"
	"errors"
	"io"
	"log"
	"strconv"

	"github.com/spiceai/spice/pkg/observations"
)

func ProcessCsv(input io.Reader) ([]observations.Observation, error) {

	reader := csv.NewReader(input)
	headers, err := reader.Read()
	if err != nil {
		return nil, errors.New("failed to read header")
	}

	records, err := reader.ReadAll()
	if err != nil {
		return nil, errors.New("failed to read lines")
	}

	newObservations := make([]observations.Observation, 0, len(records))

	if len(headers) <= 1 || len(records) == 0 {
		return newObservations, nil
	}

	if headers[0] != "time" {
		return nil, errors.New("first column must be 'time'")
	}

	// TODO: Verbose log
	// log.Printf("Read headers of %v", headers)

	for line, record := range records {
		ts, err := strconv.ParseUint(record[0], 10, 64)
		if err != nil {
			log.Printf("ignoring invalid line %d - %v: %v", line+1, record, err)
			continue
		}

		var data = make(map[string]interface{})

		for col, field := range record {
			if col == 0 || field == "" {
				continue
			}

			val, err := strconv.ParseFloat(field, 64)
			if err != nil {
				log.Printf("ignoring invalid field %d - %v: %v", line+1, field, err)
				continue
			}
			data[headers[col]] = val
		}

		if len(data) == 0 {
			continue
		}

		observation := &observations.Observation{
			Timestamp: ts,
			Data:      data,
		}

		newObservations = append(newObservations, *observation)
	}

	return newObservations, nil
}
