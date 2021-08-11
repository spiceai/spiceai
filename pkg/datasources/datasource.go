package datasources

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/spiceai/spice/pkg/connectors"
	"github.com/spiceai/spice/pkg/spec"
	"github.com/spiceai/spice/pkg/state"
)

type DataSource struct {
	spec.DataSourceSpec
	connector        connectors.Connector
	cachedState      []*state.State
	cachedStateMutex *sync.RWMutex
}

func NewDataSource(dsSpec spec.DataSourceSpec) (*DataSource, error) {
	if dsSpec.Connector == nil {
		return nil, fmt.Errorf("a connector is required for datasource %s/%s", dsSpec.From, dsSpec.Name)
	}

	connector, err := connectors.NewConnector(dsSpec.Connector.Type, dsSpec.Connector.Params)
	if err != nil {
		return nil, err
	}

	err = connector.Initialize()
	if err != nil {
		return nil, fmt.Errorf("data connector '%s' failed to initialize: %s", connector.Type(), err)
	}

	ds := DataSource{
		DataSourceSpec:   dsSpec,
		connector:        connector,
		cachedStateMutex: &sync.RWMutex{},
	}

	return &ds, nil
}

func (ds *DataSource) Name() string {
	return fmt.Sprintf("%s/%s", ds.DataSourceSpec.From, ds.DataSourceSpec.Name)
}

func (ds *DataSource) Path() string {
	return fmt.Sprintf("%s.%s", ds.DataSourceSpec.From, ds.DataSourceSpec.Name)
}

func (ds *DataSource) CachedState() []*state.State {
	return ds.cachedState
}

func (ds *DataSource) Actions() map[string]string {
	fqActions := make(map[string]string)
	fqFieldNames := ds.FieldNameMap()
	fqActionNames := ds.ActionNames()
	for dsActionName, dsActionBody := range ds.DataSourceSpec.Actions {
		fqDsActionBody := dsActionBody
		for fieldName, fqFieldName := range fqFieldNames {
			fqDsActionBody = strings.ReplaceAll(fqDsActionBody, fieldName, fqFieldName)
			fqActions[fqActionNames[dsActionName]] = strings.TrimSpace(fqDsActionBody)
		}
	}
	return fqActions
}

// Returns a mapping of fully-qualified field names to their intializers
func (ds *DataSource) Fields() map[string]float64 {
	fqFieldInitializers := make(map[string]float64)
	fqFieldNames := ds.FieldNameMap()
	for _, field := range ds.DataSourceSpec.Fields {
		var initialValue float64 = 0
		if field.Initializer != nil {
			initialValue = *field.Initializer
		}
		fqFieldInitializers[fqFieldNames[field.Name]] = initialValue
	}
	return fqFieldInitializers
}

// Returns a mapping of the datasource local field names to their fully-qualified field name
func (ds *DataSource) FieldNameMap() map[string]string {
	fieldNames := make(map[string]string, len(ds.DataSourceSpec.Fields))
	for _, v := range ds.DataSourceSpec.Fields {
		fqname := fmt.Sprintf("%s.%s.%s", ds.From, ds.DataSourceSpec.Name, v.Name)
		fieldNames[v.Name] = fqname
	}
	return fieldNames
}

func (ds *DataSource) FieldNames() []string {
	fieldNames := make([]string, len(ds.DataSourceSpec.Fields))
	for i, v := range ds.DataSourceSpec.Fields {
		fieldNames[i] = v.Name
	}
	return fieldNames
}

func (ds *DataSource) ActionNames() map[string]string {
	fqActionNames := make(map[string]string)

	for dsActionName := range ds.DataSourceSpec.Actions {
		fqName := fmt.Sprintf("%s.%s.%s", ds.DataSourceSpec.From, ds.DataSourceSpec.Name, dsActionName)
		fqActionNames[dsActionName] = fqName
	}

	return fqActionNames
}

func (ds *DataSource) Laws() []string {
	var fqLaws []string

	fqFieldNames := ds.FieldNameMap()

	for _, dsLaw := range ds.DataSourceSpec.Laws {
		law := dsLaw
		for fieldName, fqFieldName := range fqFieldNames {
			law = strings.ReplaceAll(law, fieldName, fqFieldName)
		}
		fqLaws = append(fqLaws, law)
	}

	return fqLaws
}

func (ds *DataSource) AddNewState(state *state.State) {
	ds.cachedStateMutex.Lock()
	defer ds.cachedStateMutex.Unlock()

	ds.cachedState = append(ds.cachedState, state)
}

func (ds *DataSource) FetchNewState(epoch time.Time, period time.Duration, interval time.Duration) ([]*state.State, error) {
	observations, err := ds.connector.FetchData(epoch, period, interval)
	if err != nil {
		return nil, err
	}

	newState := state.NewState(ds.Path(), ds.FieldNames(), observations)
	ds.AddNewState(newState)

	return []*state.State{newState}, nil
}
