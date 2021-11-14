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

type IdentifierInfo struct {
	Name   string
	FqName string
}

type MeasurementInfo struct {
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

type DataInfo struct {
	connectorSpec *spec.DataConnectorSpec
	connector     dataconnectors.DataConnector
	processor     dataprocessors.DataProcessor
}

type Dataspace struct {
	spec.DataspaceSpec

	seedDataInfo *DataInfo
	dataInfo     *DataInfo

	identifiers []*IdentifierInfo
	categories  []*CategoryInfo

	identifiersNames []string
	measurementNames []string
	categoryNames    []string

	tags   []string
	fqTags []string

	stateMutex    *sync.RWMutex
	cachedState   []*state.State
	stateHandlers []state.StateHandler
}

func NewDataspace(dsSpec spec.DataspaceSpec) (*Dataspace, error) {
	identifiersNames, identifiers, identifierSelectors := getIdentifiers(dsSpec)
	categoryNames, categories, categorySelectors := getCategories(dsSpec)
	measurementNames, measurementSelectors := getMeasurements(dsSpec)
	tags, fqTags := getTags(dsSpec)

	ds := Dataspace{
		DataspaceSpec:    dsSpec,
		stateMutex:       &sync.RWMutex{},
		identifiers:      identifiers,
		identifiersNames: identifiersNames,
		measurementNames: measurementNames,
		categories:       categories,
		categoryNames:    categoryNames,
		tags:             tags,
		fqTags:           fqTags,
	}

	tagSelectors := []string{"_tags"}
	if dsSpec.Tags != nil {
		tagSelectors = append(tagSelectors, dsSpec.Tags.Selectors...)
	}

	if dsSpec.SeedData != nil {
		dataInfo, err := getDataInfo(dsSpec.SeedData, identifierSelectors, measurementSelectors, categorySelectors, tagSelectors, ds.ReadSeedData)
		if err != nil {
			return nil, err
		}
		ds.seedDataInfo = dataInfo
	}

	if dsSpec.Data != nil {
		dataInfo, err := getDataInfo(dsSpec.Data, identifierSelectors, measurementSelectors, categorySelectors, tagSelectors, ds.ReadData)
		if err != nil {
			return nil, err
		}
		ds.dataInfo = dataInfo
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

// Returns the list of Identifiers sorted by Name
func (ds *Dataspace) Identifiers() []*IdentifierInfo {
	return ds.identifiers
}

// Returns the list of Categories sorted by Name
func (ds *Dataspace) Categories() []*CategoryInfo {
	return ds.categories
}

// Returns a mapping of fully-qualified measurement names to Measurements
func (ds *Dataspace) Measurements() map[string]*MeasurementInfo {
	fqMeasurementInitializers := make(map[string]*MeasurementInfo)
	fqMeasurementNames := ds.MeasurementNameMap()
	for _, measurementSpec := range ds.DataspaceSpec.Measurements {
		measurement := &MeasurementInfo{
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
		fqname := fmt.Sprintf("%s.%s.%s", ds.DataspaceSpec.From, ds.DataspaceSpec.Name, v.Name)
		measurementNames[v.Name] = fqname
	}
	return measurementNames
}

// Returns the sorted list of local identifiers names
func (ds *Dataspace) IdentifiersNames() []string {
	return ds.identifiersNames
}

// Returns the sorted list of local measurement names
func (ds *Dataspace) MeasurementNames() []string {
	return ds.measurementNames
}

// Returns the sorted list of local category names
func (ds *Dataspace) CategoryNames() []string {
	return ds.categoryNames
}

// Returns the list of fully-qualified tags
func (ds *Dataspace) FqTags() []string {
	return ds.fqTags
}

// Returns the local tag name (not fully-qualified)
func (ds *Dataspace) Tags() []string {
	return ds.tags
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
	if ds.seedDataInfo != nil && ds.seedDataInfo.connector != nil {
		if err := ds.seedDataInfo.connector.Init(epoch, period, interval, ds.seedDataInfo.connectorSpec.Params); err != nil {
			return fmt.Errorf("failed to initialize seed data connector '%s': %s", ds.seedDataInfo.connectorSpec.Name, err)
		}
	}

	if ds.dataInfo != nil && ds.dataInfo.connector != nil {
		if err := ds.dataInfo.connector.Init(epoch, period, interval, ds.dataInfo.connectorSpec.Params); err != nil {
			return fmt.Errorf("failed to initialize data connector '%s': %s", ds.dataInfo.connectorSpec.Name, err)
		}
	}

	return nil
}

func (ds *Dataspace) ReadSeedData(data []byte, metadata map[string]string) ([]byte, error) {
	return ds.readData(ds.seedDataInfo.processor, data, metadata)
}

func (ds *Dataspace) ReadData(data []byte, metadata map[string]string) ([]byte, error) {
	return ds.readData(ds.dataInfo.processor, data, metadata)
}

func (ds *Dataspace) readData(processor dataprocessors.DataProcessor, data []byte, metadata map[string]string) ([]byte, error) {
	if data == nil {
		return nil, nil
	}

	_, err := processor.OnData(data)
	if err != nil {
		return nil, err
	}

	observations, err := processor.GetObservations()
	if err != nil {
		return nil, err
	}

	newState := state.NewState(ds.Path(), ds.IdentifiersNames(), ds.MeasurementNames(), ds.CategoryNames(), ds.Tags(), observations)
	err = ds.AddNewState(newState, metadata)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func getDataInfo(dataSpec *spec.DataSpec, identifierSelectors map[string]string, measurementSelectors map[string]string, categorySelectors map[string]string, tagSelectors []string, readData func(data []byte, metadata map[string]string) ([]byte, error)) (*DataInfo, error) {
	processor, err := dataprocessors.NewDataProcessor(dataSpec.Processor.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize data processor '%s': %s", dataSpec.Processor.Name, err)
	}

	err = processor.Init(dataSpec.Processor.Params, identifierSelectors, measurementSelectors, categorySelectors, tagSelectors)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize data processor '%s': %s", dataSpec.Processor.Name, err)
	}

	var connector dataconnectors.DataConnector
	if dataSpec.Connector.Name != "" {
		connector, err = dataconnectors.NewDataConnector(dataSpec.Connector.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to initialize data connector '%s': %s", dataSpec.Connector.Name, err)
		}

		err = connector.Read(readData)
		if err != nil {
			return nil, fmt.Errorf("'%s' data connector failed to read: %s", dataSpec.Connector.Name, err)
		}
	}

	return &DataInfo{
		connectorSpec: &dataSpec.Connector,
		connector:     connector,
		processor:     processor,
	}, nil
}

func getIdentifiers(dsSpec spec.DataspaceSpec) ([]string, []*IdentifierInfo, map[string]string) {
	identifierNames := make([]string, len(dsSpec.Identifiers))
	identifiers := make([]*IdentifierInfo, len(dsSpec.Identifiers))
	identifierSelectors := make(map[string]string)
	for i, identifierSpec := range dsSpec.Identifiers {
		identifierNames[i] = identifierSpec.Name
		fqIdentifierName := fmt.Sprintf("%s.%s.%s", dsSpec.From, dsSpec.Name, identifierSpec.Name)
		identifiers[i] = &IdentifierInfo{
			Name:   identifierSpec.Name,
			FqName: fqIdentifierName,
		}
		if identifierSpec.Selector == "" {
			identifierSelectors[identifierSpec.Name] = identifierSpec.Name
		} else {
			identifierSelectors[identifierSpec.Name] = strings.TrimSpace(identifierSpec.Selector)
		}
	}
	sort.Strings(identifierNames)
	sort.SliceStable(identifiers, func(i, j int) bool {
		return strings.Compare(identifiers[i].Name, identifiers[j].Name) == -1
	})
	return identifierNames, identifiers, identifierSelectors
}

func getMeasurements(dsSpec spec.DataspaceSpec) ([]string, map[string]string) {
	measurementNames := make([]string, 0, len(dsSpec.Measurements))
	measurementSelectors := make(map[string]string)
	for _, v := range dsSpec.Measurements {
		measurementNames = append(measurementNames, v.Name)
		if v.Selector == "" {
			measurementSelectors[v.Name] = v.Name
		} else {
			measurementSelectors[v.Name] = strings.TrimSpace(v.Selector)
		}
	}
	sort.Strings(measurementNames)

	return measurementNames, measurementSelectors
}

func getCategories(dsSpec spec.DataspaceSpec) ([]string, []*CategoryInfo, map[string]string) {
	categoryNames := make([]string, len(dsSpec.Categories))
	categories := make([]*CategoryInfo, len(dsSpec.Categories))
	categorySelectors := make(map[string]string)
	for i, categorySpec := range dsSpec.Categories {
		categoryNames[i] = categorySpec.Name
		fqCategoryName := fmt.Sprintf("%s.%s.%s", dsSpec.From, dsSpec.Name, categorySpec.Name)
		sort.Strings(categorySpec.Values)
		fieldNames := make([]string, len(categorySpec.Values))
		for i, val := range categorySpec.Values {
			oneHotFieldName := fmt.Sprintf("%s-%s", fqCategoryName, val)
			oneHotFieldName = strings.ReplaceAll(oneHotFieldName, ".", "_")
			fieldNames[i] = oneHotFieldName
		}
		categories[i] = &CategoryInfo{
			Name:              categorySpec.Name,
			FqName:            fqCategoryName,
			Values:            categorySpec.Values,
			EncodedFieldNames: fieldNames,
		}
		if categorySpec.Selector == "" {
			categorySelectors[categorySpec.Name] = categorySpec.Name
		} else {
			categorySelectors[categorySpec.Name] = strings.TrimSpace(categorySpec.Selector)
		}
	}
	sort.Strings(categoryNames)
	sort.SliceStable(categories, func(i, j int) bool {
		return strings.Compare(categories[i].Name, categories[j].Name) == -1
	})
	return categoryNames, categories, categorySelectors
}

func getTags(dsSpec spec.DataspaceSpec) ([]string, []string) {
	numTags := 0
	if dsSpec.Tags != nil {
		numTags = len(dsSpec.Tags.Values)
	}
	tags := make([]string, numTags)
	fqTags := make([]string, numTags)
	if numTags > 0 {
		for i, tagName := range dsSpec.Tags.Values {
			tags[i] = tagName
			fqTags[i] = fmt.Sprintf("%s.%s.%s", dsSpec.From, dsSpec.Name, tagName)
		}
		sort.Strings(tags)
		sort.Strings(fqTags)
	}
	return tags, fqTags
}

func getTagSelectors(dsSpec spec.DataspaceSpec) []string {
	numSelectors := 0
	if dsSpec.Tags != nil {
		numSelectors = len(dsSpec.Tags.Selectors)
	}
	tagSelectors := make([]string, numSelectors+1)
	if numSelectors > 0 {
		for i, tagSelector := range dsSpec.Tags.Selectors {
			tagSelectors[i] = tagSelector
		}
	}
	tagSelectors[numSelectors] = "_tag"
	sort.Strings(tagSelectors)
	return tagSelectors
}
