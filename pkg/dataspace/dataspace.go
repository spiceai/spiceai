package dataspace

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/spiceai/data-components-contrib/dataconnectors"
	"github.com/spiceai/data-components-contrib/dataprocessors"
	"github.com/spiceai/spiceai/pkg/spec"
	"github.com/spiceai/spiceai/pkg/state"
	"golang.org/x/sync/errgroup"
)

type Dataspace struct {
	spec.DataspaceSpec
	connector dataconnectors.DataConnector
	processor dataprocessors.DataProcessor

	stateMutex    *sync.RWMutex
	cachedState   []*state.State
	stateHandlers []state.StateHandler
}

func NewDataspace(dsSpec spec.DataspaceSpec) (*Dataspace, error) {
	ds := Dataspace{
		DataspaceSpec: dsSpec,
		stateMutex:    &sync.RWMutex{},
	}

	if dsSpec.Data != nil {
		var connector dataconnectors.DataConnector = nil
		var err error

		processor, err := dataprocessors.NewDataProcessor(ds.Data.Processor.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize data processor '%s': %s", dsSpec.Data.Connector.Name, err)
		}

		err = processor.Init(dsSpec.Data.Connector.Params)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize data processor '%s': %s", dsSpec.Data.Connector.Name, err)
		}

		ds.processor = processor

		if dsSpec.Data.Connector.Name != "" {
			connector, err = dataconnectors.NewDataConnector(dsSpec.Data.Connector.Name)
			if err != nil {
				return nil, fmt.Errorf("failed to initialize data connector '%s': %s", dsSpec.Data.Connector.Name, err)
			}

			err = connector.Read(ds.ReadData)
			if err != nil {
				return nil, fmt.Errorf("failed to initialize data connector '%s': %s", dsSpec.Data.Connector.Name, err)
			}
		}

		ds.connector = connector
	}

	return &ds, nil
}

func (ds *Dataspace) Name() string {
	return fmt.Sprintf("%s/%s", ds.DataspaceSpec.From, ds.DataspaceSpec.Name)
}

func (ds *Dataspace) Path() string {
	return fmt.Sprintf("%s.%s", ds.DataspaceSpec.From, ds.DataspaceSpec.Name)
}

func (ds *Dataspace) CachedState() []*state.State {
	return ds.cachedState
}

func (ds *Dataspace) Actions() map[string]string {
	fqActions := make(map[string]string)
	fqFieldNames := ds.FieldNameMap()
	fqActionNames := ds.ActionNames()
	for dsActionName, dsActionBody := range ds.DataspaceSpec.Actions {
		fqDsActionBody := dsActionBody
		for fieldName, fqFieldName := range fqFieldNames {
			fqDsActionBody = strings.ReplaceAll(fqDsActionBody, fieldName, fqFieldName)
			fqActions[fqActionNames[dsActionName]] = strings.TrimSpace(fqDsActionBody)
		}
	}
	return fqActions
}

// Returns a mapping of fully-qualified field names to their intializers
func (ds *Dataspace) Fields() map[string]float64 {
	fqFieldInitializers := make(map[string]float64)
	fqFieldNames := ds.FieldNameMap()
	for _, field := range ds.DataspaceSpec.Fields {
		if field.Type == "tag" {
			continue
		}
		var initialValue float64 = 0
		if field.Initializer != nil {
			initialValue = *field.Initializer
		}
		fqFieldInitializers[fqFieldNames[field.Name]] = initialValue
	}
	return fqFieldInitializers
}

// Returns a mapping of the datasource local field names to their fully-qualified field name
func (ds *Dataspace) FieldNameMap() map[string]string {
	fieldNames := make(map[string]string, len(ds.DataspaceSpec.Fields))
	for _, v := range ds.DataspaceSpec.Fields {
		if v.Type == "tag" {
			continue
		}
		fqname := fmt.Sprintf("%s.%s.%s", ds.From, ds.DataspaceSpec.Name, v.Name)
		fieldNames[v.Name] = fqname
	}
	return fieldNames
}

func (ds *Dataspace) FieldNames() []string {
	fieldNames := make([]string, 0, len(ds.DataspaceSpec.Fields))
	for _, v := range ds.DataspaceSpec.Fields {
		if v.Type == "tag" {
			continue
		}
		fieldNames = append(fieldNames, v.Name)
	}

	return fieldNames
}

// Returns the local tag name (not fully-qualified)
func (ds *Dataspace) Tags() []string {
	tags := make([]string, 0)
	for _, v := range ds.DataspaceSpec.Fields {
		if v.Type == "tag" {
			tags = append(tags, v.Name)
		}
	}

	return tags
}

func (ds *Dataspace) ActionNames() map[string]string {
	fqActionNames := make(map[string]string)

	for dsActionName := range ds.DataspaceSpec.Actions {
		fqName := fmt.Sprintf("%s.%s.%s", ds.DataspaceSpec.From, ds.DataspaceSpec.Name, dsActionName)
		fqActionNames[dsActionName] = fqName
	}

	return fqActionNames
}

func (ds *Dataspace) Laws() []string {
	var fqLaws []string

	fqFieldNames := ds.FieldNameMap()

	for _, dsLaw := range ds.DataspaceSpec.Laws {
		law := dsLaw
		for fieldName, fqFieldName := range fqFieldNames {
			law = strings.ReplaceAll(law, fieldName, fqFieldName)
		}
		fqLaws = append(fqLaws, law)
	}

	return fqLaws
}

func (ds *Dataspace) AddNewState(state *state.State, metadata map[string]string) error {
	ds.stateMutex.Lock()
	defer ds.stateMutex.Unlock()

	ds.cachedState = append(ds.cachedState, state)

	errGroup, _ := errgroup.WithContext(context.Background())

	for _, handler := range ds.stateHandlers {
		h := handler
		errGroup.Go(func() error {
			return h(state, metadata)
		})
	}

	return errGroup.Wait()
}

func (ds *Dataspace) RegisterStateHandler(handler func(state *state.State, metadata map[string]string) error) {
	ds.stateMutex.Lock()
	defer ds.stateMutex.Unlock()

	ds.stateHandlers = append(ds.stateHandlers, handler)
}

func (ds *Dataspace) InitDataConnector(epoch time.Time, period time.Duration, interval time.Duration) error {
	if ds.connector != nil {
		err := ds.connector.Init(epoch, period, interval, ds.Data.Connector.Params)
		if err != nil {
			return fmt.Errorf("failed to initialize data connector '%s': %s", ds.Data.Connector.Name, err)
		}
	}
	return nil
}

func (ds *Dataspace) ReadData(data []byte, metadata map[string]string) ([]byte, error) {
	if data == nil {
		return nil, nil
	}

	_, err := ds.processor.OnData(data)
	if err != nil {
		return nil, err
	}

	observations, err := ds.processor.GetObservations()
	if err != nil {
		return nil, err
	}

	newState := state.NewState(ds.Path(), ds.FieldNames(), ds.Tags(), observations)
	err = ds.AddNewState(newState, metadata)
	if err != nil {
		return nil, err
	}

	return data, nil
}
