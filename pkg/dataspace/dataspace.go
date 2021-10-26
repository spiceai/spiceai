package dataspace

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/spiceai/data-components-contrib/dataconnectors"
	"github.com/spiceai/data-components-contrib/dataprocessors"
	"github.com/spiceai/spiceai/pkg/spec"
	"github.com/spiceai/spiceai/pkg/state"
	"golang.org/x/sync/errgroup"
)

type Measurement struct {
	Name         string
	InitialValue float64
	Fill         string
}

type CategoryInfo struct {
	Name              string
	FqName            string
	Values            []string
	EncodedFieldNames []string
}

type Dataspace struct {
	spec.DataspaceSpec
	connector dataconnectors.DataConnector
	processor dataprocessors.DataProcessor

	categories       []*CategoryInfo
	measurementNames []string
	fqTags           []string

	stateMutex    *sync.RWMutex
	cachedState   []*state.State
	stateHandlers []state.StateHandler
}

func NewDataspace(dsSpec spec.DataspaceSpec) (*Dataspace, error) {
	categories, categorySelectors := getCategories(dsSpec)
	measurementNames, measurementSelectors := getMeasurements(dsSpec)
	fqTags := getTags(dsSpec)

	ds := Dataspace{
		DataspaceSpec:    dsSpec,
		stateMutex:       &sync.RWMutex{},
		categories:       categories,
		measurementNames: measurementNames,
		fqTags:           fqTags,
	}

	if dsSpec.Data != nil {
		var connector dataconnectors.DataConnector = nil
		var err error

		processor, err := dataprocessors.NewDataProcessor(ds.Data.Processor.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize data processor '%s': %s", dsSpec.Data.Connector.Name, err)
		}

		err = processor.Init(dsSpec.Data.Connector.Params, measurementSelectors, categorySelectors)
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
				return nil, fmt.Errorf("'%s' data connector failed to read: %s", dsSpec.Data.Connector.Name, err)
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
	fqMeasurementNames := ds.MeasurementNameMap()
	fqActionNames := ds.ActionNames()
	for dsActionName, dsActionBody := range ds.DataspaceSpec.Actions {
		fqDsActionBody := dsActionBody
		for measurementName, fqMeasurementName := range fqMeasurementNames {
			fqDsActionBody = strings.ReplaceAll(fqDsActionBody, measurementName, fqMeasurementName)
			fqActions[fqActionNames[dsActionName]] = strings.TrimSpace(fqDsActionBody)
		}
	}
	return fqActions
}

// Returns the list of Categories sorted by Name
func (ds *Dataspace) Categories() []*CategoryInfo {
	return ds.categories
}

// Returns a mapping of fully-qualified measurement names to Measurements
func (ds *Dataspace) Measurements() map[string]*Measurement {
	fqMeasurementInitializers := make(map[string]*Measurement)
	fqMeasurementNames := ds.MeasurementNameMap()
	for _, measurementSpec := range ds.DataspaceSpec.Measurements {
		measurement := &Measurement{
			Name:         measurementSpec.Name,
			InitialValue: 0,
			Fill:         measurementSpec.Fill,
		}
		if measurementSpec.Initializer != nil {
			measurement.InitialValue = *measurementSpec.Initializer
		}
		fqMeasurementInitializers[fqMeasurementNames[measurementSpec.Name]] = measurement
	}
	return fqMeasurementInitializers
}

// Returns a mapping of the datasource local measurement names to their fully-qualified measurement name
func (ds *Dataspace) MeasurementNameMap() map[string]string {
	measurementNames := make(map[string]string, len(ds.DataspaceSpec.Measurements))
	for _, v := range ds.DataspaceSpec.Measurements {
		fqname := fmt.Sprintf("%s.%s.%s", ds.From, ds.DataspaceSpec.Name, v.Name)
		measurementNames[v.Name] = fqname
	}
	return measurementNames
}

// Returns the sorted list of loca measurement names
func (ds *Dataspace) MeasurementNames() []string {
	return ds.measurementNames
}

// Returns the list of fully-qualified tags
func (ds *Dataspace) FqTags() []string {
	return ds.fqTags
}

// Returns the local tag name (not fully-qualified)
func (ds *Dataspace) Tags() []string {
	return ds.DataspaceSpec.Tags
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

	fqMeasurementNames := ds.MeasurementNameMap()

	for _, dsLaw := range ds.DataspaceSpec.Laws {
		law := dsLaw
		for measurementName, fqMeasurementName := range fqMeasurementNames {
			law = strings.ReplaceAll(law, measurementName, fqMeasurementName)
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

	newState := state.NewState(ds.Path(), ds.MeasurementNames(), ds.Tags(), observations)
	err = ds.AddNewState(newState, metadata)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func getMeasurements(dsSpec spec.DataspaceSpec) ([]string, map[string]string) {
	measurementNames := make([]string, 0, len(dsSpec.Measurements))
	measurementSelectors := make(map[string]string)
	for _, v := range dsSpec.Measurements {
		measurementNames = append(measurementNames, v.Name)
		if v.Selector == "" {
			measurementSelectors[v.Name] = v.Name
		} else {
			measurementSelectors[v.Name] = v.Selector
		}
	}
	sort.Strings(measurementNames)

	return measurementNames, measurementSelectors
}

func getCategories(dsSpec spec.DataspaceSpec) ([]*CategoryInfo, map[string]string) {
	categories := make([]*CategoryInfo, len(dsSpec.Categories))
	categorySelectors := make(map[string]string)
	for i, categorySpec := range dsSpec.Categories {
		fqCategoryName := fmt.Sprintf("%s.%s.%s", dsSpec.From, dsSpec.Name, categorySpec.Name)
		sort.Strings(categorySpec.Values)
		fieldNames := make([]string, len(categorySpec.Values))
		for i, val := range categorySpec.Values {
			oneHotFieldName := fmt.Sprintf("%s-%s", fqCategoryName, val)
			oneHotFieldName = strings.ReplaceAll(oneHotFieldName, ".", "_")
			fieldNames[i] = oneHotFieldName
		}
		categories[i] = &CategoryInfo{
			Name:   categorySpec.Name,
			FqName: fqCategoryName,
			Values: categorySpec.Values,
			EncodedFieldNames: fieldNames,
		}
		if categorySpec.Selector == "" {
			categorySelectors[categorySpec.Name] = categorySpec.Name
		} else {
			categorySelectors[categorySpec.Name] = categorySpec.Selector
		}
	}
	sort.SliceStable(categories, func(i, j int) bool {
		return strings.Compare(categories[i].Name, categories[j].Name) == -1
	})
	return categories, categorySelectors
}

func getTags(dsSpec spec.DataspaceSpec) []string {
	fqTags := make([]string, len(dsSpec.Tags))
	for i, tagName := range dsSpec.Tags {
		fqTags[i] = fmt.Sprintf("%s.%s.%s", dsSpec.From, dsSpec.Name, tagName)
	}
	sort.Strings(fqTags)
	return fqTags
}
