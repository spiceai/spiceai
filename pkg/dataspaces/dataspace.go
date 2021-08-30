package dataspaces

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/spiceai/spice/pkg/dataconnectors"
	"github.com/spiceai/spice/pkg/dataprocessors"
	"github.com/spiceai/spice/pkg/spec"
	"github.com/spiceai/spice/pkg/state"
)

type Dataspace struct {
	spec.DataspaceSpec
	connector        dataconnectors.DataConnector
	processor        dataprocessors.DataProcessor
	cachedState      []*state.State
	cachedStateMutex *sync.RWMutex
}

func NewDataspace(dsSpec spec.DataspaceSpec) (*Dataspace, error) {
	ds := Dataspace{
		DataspaceSpec:    dsSpec,
		cachedStateMutex: &sync.RWMutex{},
	}

	if dsSpec.Data != nil {
		connector, err := dataconnectors.NewDataConnector(dsSpec.Data.Connector.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize data connector '%s': %s", dsSpec.Data.Connector.Name, err)
		}

		err = connector.Init(dsSpec.Data.Connector.Params)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize data connector '%s': %s", dsSpec.Data.Connector.Name, err)
		}

		processor, err := dataprocessors.NewDataProcessor(ds.Data.Processor.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize data processor '%s': %s", dsSpec.Data.Connector.Name, err)
		}

		err = processor.Init(dsSpec.Data.Connector.Params)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize data processor '%s': %s", dsSpec.Data.Connector.Name, err)
		}

		ds.connector = connector
		ds.processor = processor
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
		fqname := fmt.Sprintf("%s.%s.%s", ds.From, ds.DataspaceSpec.Name, v.Name)
		fieldNames[v.Name] = fqname
	}
	return fieldNames
}

func (ds *Dataspace) FieldNames() []string {
	fieldNames := make([]string, len(ds.DataspaceSpec.Fields))
	for i, v := range ds.DataspaceSpec.Fields {
		fieldNames[i] = v.Name
	}
	return fieldNames
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

func (ds *Dataspace) AddNewState(state *state.State) {
	ds.cachedStateMutex.Lock()
	defer ds.cachedStateMutex.Unlock()

	ds.cachedState = append(ds.cachedState, state)
}

func (ds *Dataspace) FetchNewState(epoch time.Time, period time.Duration, interval time.Duration) ([]*state.State, error) {
	if ds.connector == nil || ds.processor == nil {
		return nil, nil
	}

	data, err := ds.connector.FetchData(epoch, period, interval)
	if err != nil {
		return nil, err
	}

	if data == nil {
		return nil, nil
	}

	_, err = ds.processor.OnData(data)
	if err != nil {
		return nil, err
	}

	observations, err := ds.processor.GetObservations()
	if err != nil {
		return nil, err
	}

	newState := state.NewState(ds.Path(), ds.FieldNames(), observations)
	ds.AddNewState(newState)

	return []*state.State{newState}, nil
}
