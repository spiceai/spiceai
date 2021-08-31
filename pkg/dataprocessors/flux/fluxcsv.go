package flux

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/influxdata/flux"
	flux_csv "github.com/influxdata/flux/csv"
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
	FluxCsvProcessorName string = "flux-csv"
)

type FluxCsvProcessor struct {
	data      []byte
	dataMutex sync.RWMutex
	dataHash  []byte
}

func NewFluxCsvProcessor() *FluxCsvProcessor {
	return &FluxCsvProcessor{}
}

func (p *FluxCsvProcessor) Init(params map[string]string) error {
	return nil
}

func (p *FluxCsvProcessor) OnData(data []byte) ([]byte, error) {
	p.dataMutex.Lock()
	defer p.dataMutex.Unlock()

	newDataHash, err := util.ComputeNewHash(p.data, p.dataHash, data)
	if err != nil {
		return nil, fmt.Errorf("error computing new data hash in %s processor: %w", FluxCsvProcessorName, err)
	}

	if newDataHash != nil {
		// Only update data if new
		p.data = data
		p.dataHash = newDataHash
	}

	return data, nil
}

func (p *FluxCsvProcessor) GetObservations() ([]observations.Observation, error) {
	p.dataMutex.Lock()
	defer p.dataMutex.Unlock()

	if len(p.data) == 0 {
		return nil, nil
	}

	reader := bytes.NewReader(p.data)
	readCloser := io.NopCloser(reader)

	decoder := flux_csv.NewMultiResultDecoder(flux_csv.ResultDecoderConfig{ /* Use defaults */ })
	results, err := decoder.Decode(readCloser)
	if err != nil {
		return nil, err
	}
	defer results.Release()

	var newObservations []observations.Observation

	for results.More() {
		result := results.Next()

		err = result.Tables().Do(func(t flux.Table) error {
			return t.Do(func(c flux.ColReader) error {
				tableObservations := make([]observations.Observation, c.Len())
				timeCol := -1
				fieldCol := -1
				valueCol := -1
				for col, colMeta := range c.Cols() {
					// We currently only support one field and float for now
					if colMeta.Label == "_time" {
						timeCol = col
						continue
					}

					if colMeta.Label == "_field" {
						fieldCol = col
						continue
					}

					if colMeta.Label == "_value" {
						valueCol = col
						continue
					}

					if timeCol >= 0 && fieldCol >= 0 && valueCol >= 0 {
						break
					}
				}

				if timeCol == -1 {
					return errors.New("'_time' not found in table data")
				}

				if fieldCol == -1 {
					return errors.New("'_field' not found in table data")
				}

				if valueCol == -1 {
					return errors.New("'_value' not found in table data")
				}

				times := c.Times(timeCol)
				defer times.Release()

				fields := c.Strings(fieldCol)
				defer fields.Release()

				values := c.Floats(valueCol)
				defer values.Release()

				for i := 0; i < c.Len(); i++ {
					if times.IsValid(i) && !times.IsNull(i) &&
						fields.IsValid(i) && !fields.IsNull(i) &&
						values.IsValid(i) && !values.IsNull(i) {
						rowData := make(map[string]float64, 1)
						rowData[fields.Value(i)] = values.Value(i)
						observation := observations.Observation{
							Time: times.Value(i) / int64(time.Second),
							Data: rowData,
						}
						tableObservations[i] = observation
					}
				}

				defer c.Release()

				newObservations = append(newObservations, tableObservations...)

				return nil
			})
		})
		if err != nil {
			return nil, err
		}
	}

	err = results.Err()
	if err != nil {
		zaplog.Sugar().Warnf("error decoding flux csv result: %w", err)
		return nil, err
	}

	p.data = nil

	return newObservations, nil
}

func (p *FluxCsvProcessor) GetState(validFields *[]string) ([]*state.State, error) {
	// TODO
	return nil, nil
}
