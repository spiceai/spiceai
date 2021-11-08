package pods

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
	"github.com/spiceai/spiceai/pkg/constants"
	"github.com/spiceai/spiceai/pkg/dataspace"
	"github.com/spiceai/spiceai/pkg/flights"
	"github.com/spiceai/spiceai/pkg/interpretations"
	"github.com/spiceai/spiceai/pkg/observations"
	"github.com/spiceai/spiceai/pkg/spec"
	"github.com/spiceai/spiceai/pkg/state"
	spice_time "github.com/spiceai/spiceai/pkg/time"
	"github.com/spiceai/spiceai/pkg/util"
	"github.com/spiceai/spiceai/pkg/validator"
	"golang.org/x/sync/errgroup"
)

type Pod struct {
	spec.PodSpec
	viper        *viper.Viper
	podParams    *PodParams
	hash         string
	manifestPath string

	timeCategories    map[string][]spice_time.TimeCategoryInfo
	timeCategoryNames []string

	dataspaces          []*dataspace.Dataspace
	dataspaceMap        map[string]*dataspace.Dataspace
	actions             map[string]string
	measurements        map[string]*dataspace.MeasurementInfo
	fqIdentifierNames   []string
	fqMeasurementNames  []string
	fqCategoryNames     []string
	tags                []string
	externalRewardFuncs string

	flights map[string]*flights.Flight

	podLocalStateMutex    sync.RWMutex
	podLocalState         []*state.State
	podLocalStateHandlers []state.StateHandler

	interpretations *interpretations.InterpretationsStore

	fqCsvHeaders string
}

func (pod *Pod) Hash() string {
	return pod.hash
}

func (f *Pod) ManifestPath() string {
	return f.manifestPath
}

func (pod *Pod) Period() time.Duration {
	return pod.podParams.Period
}

func (pod *Pod) Interval() time.Duration {
	return pod.podParams.Interval
}

func (pod *Pod) Epoch() time.Time {
	if pod.podParams.Epoch.IsZero() {
		return time.Now().Add(-pod.Period())
	}

	return pod.podParams.Epoch
}

func (pod *Pod) Granularity() time.Duration {
	return pod.podParams.Granularity
}

func (pod *Pod) TimeCategories() map[string][]spice_time.TimeCategoryInfo {
	return pod.timeCategories
}

func (pod *Pod) TimeCategoryNames() []string {
	return pod.timeCategoryNames
}

func (pod *Pod) TrainingGoal() *string {
	if pod.PodSpec.Training == nil {
		return nil
	}

	return &pod.PodSpec.Training.Goal
}

func (pod *Pod) Episodes() int {
	if pod.PodSpec.Params != nil {
		episodesParam, ok := pod.PodSpec.Params["episodes"]
		if ok {
			if episodes, err := strconv.ParseInt(episodesParam, 0, 0); err == nil {
				return int(episodes)
			}
		}
	}
	return 10
}

func (pod *Pod) CachedState() []*state.State {
	var cachedState []*state.State
	for _, ds := range pod.Dataspaces() {
		dsState := ds.CachedState()
		if dsState != nil {
			cachedState = append(cachedState, dsState...)
		}
	}

	if pod.podLocalState != nil {
		pod.podLocalStateMutex.RLock()
		defer pod.podLocalStateMutex.RUnlock()
		if pod.podLocalState != nil && len(pod.podLocalState) > 0 {
			cachedState = append(cachedState, pod.podLocalState...)
		}
	}

	return cachedState
}

func (pod *Pod) CachedCsv() string {
	csv := strings.Builder{}

	csv.WriteString(pod.csvHeaders())

	measurementNames := pod.MeasurementNames()
	categoryNames := pod.CategoryNames()

	cachedState := pod.CachedState()
	for _, state := range cachedState {
		var validHeaders []string

		for _, podFqMeasurementName := range measurementNames {
			isLocal := false
			for measurementName, fqMeasurementName := range state.MeasurementsNamesMap() {
				if podFqMeasurementName == fqMeasurementName {
					validHeaders = append(validHeaders, measurementName)
					isLocal = true
					break
				}
			}
			if !isLocal {
				validHeaders = append(validHeaders, podFqMeasurementName)
			}
		}

		for _, podFqCategoryName := range categoryNames {
			isLocal := false
			for categoryName, fqCategoryName := range state.CategoryNamesMap() {
				if podFqCategoryName == fqCategoryName {
					validHeaders = append(validHeaders, categoryName)
					isLocal = true
					break
				}
			}
			if !isLocal {
				validHeaders = append(validHeaders, podFqCategoryName)
			}
		}

		stateCsv := observations.GetCsv(validHeaders, pod.Tags(), state.Observations())
		csv.WriteString(stateCsv)
	}
	return csv.String()
}

func (pod *Pod) Dataspaces() []*dataspace.Dataspace {
	return pod.dataspaces
}

func (pod *Pod) Flights() *map[string]*flights.Flight {
	return &pod.flights
}

func (pod *Pod) Interpretations() *interpretations.InterpretationsStore {
	return pod.interpretations
}

func (pod *Pod) GetDataspace(path string) *dataspace.Dataspace {
	return pod.dataspaceMap[path]
}

func (pod *Pod) GetFlight(flight string) *flights.Flight {
	f, ok := pod.flights[flight]
	if ok {
		return f
	}
	return nil
}

func (pod *Pod) AddFlight(flightId string, flight *flights.Flight) {
	pod.flights[flightId] = flight
}

func (pod *Pod) Actions() map[string]string {
	return pod.actions
}

func (pod *Pod) ActionsArgs() []string {
	actionsArgsMap := make(map[string]bool)
	for _, globalAction := range pod.PodSpec.Actions {
		if globalAction.Do == nil {
			continue
		}
		for argName := range globalAction.Do.Args {
			actionsArgsMap[fmt.Sprintf("args.%s", argName)] = true
		}
	}

	actionsArgs := make([]string, 0, len(actionsArgsMap))
	for arg := range actionsArgsMap {
		actionsArgs = append(actionsArgs, arg)
	}

	return actionsArgs
}

func (pod *Pod) ExternalRewardFuncs() string {
	return pod.externalRewardFuncs
}

func (pod *Pod) Rewards() map[string]string {
	rewards := make(map[string]string)

	if pod.PodSpec.Training == nil || pod.PodSpec.Training.Rewards == nil || pod.PodSpec.Training.Rewards == "uniform" {
		for actionName := range pod.Actions() {
			rewards[actionName] = "reward = 1"
		}
		return rewards
	}

	rewardSpecs, err := pod.loadRewardSpecs()
	if err != nil {
		for actionName := range pod.Actions() {
			rewards[actionName] = "reward = 1"
		}
		return rewards
	}

	for _, reward := range rewardSpecs {
		rewards[reward.Reward] = reward.With
	}

	return rewards
}

func (pod *Pod) Measurements() map[string]*dataspace.MeasurementInfo {
	return pod.measurements
}

// Returns the list of fully-qualified identifier names
func (pod *Pod) IdentifierNames() []string {
	return pod.fqIdentifierNames
}

// Returns the list of fully-qualified measurement names
func (pod *Pod) MeasurementNames() []string {
	return pod.fqMeasurementNames
}

// Returns the list of fully-qualified category names
func (pod *Pod) CategoryNames() []string {
	return pod.fqCategoryNames
}

// Returns the global list of tag values
func (pod *Pod) Tags() []string {
	return pod.tags
}

func (pod *Pod) ValidateForTraining() error {
	// Consider using something like https://github.com/go-playground/validator in the future
	if pod.Granularity() > pod.Interval() {
		return errors.New("granularity must be less than or equal to interval")
	}

	if pod.Interval() > pod.Period() {
		return errors.New("interval must be less than or equal to period")
	}

	if pod.PodSpec.Dataspaces == nil || len(pod.PodSpec.Dataspaces) < 1 {
		return errors.New("at least one dataspace is required for training")
	}

	for _, ds := range pod.PodSpec.Dataspaces {
		valid := validator.ValidateDataspaceName(ds.From)
		if !valid {
			return fmt.Errorf("invalid dataspace \"from\": '%s' should only contain A-Za-z0-9_", ds.From)
		}
		valid = validator.ValidateDataspaceName(ds.Name)
		if !valid {
			return fmt.Errorf("invalid dataspace \"name\": '%s' should only contain A-Za-z0-9_", ds.Name)
		}

		for _, f := range ds.Measurements {
			switch f.Fill {
			case "":
			case "previous":
			case "none":
			default:
				return fmt.Errorf("invalid measurement fill '%s': choose one of ['previous', 'none']", f.Fill)
			}
		}
	}

	actions := pod.Actions()

	if len(actions) == 0 {
		return errors.New("at least one action is required for training")
	}

	// Check for args.<arg name>
	rewards := pod.Rewards()

	for actionName, action := range actions {
		numErrors := 0
		matches := validator.GetArgsRegex().FindStringSubmatch(action)
		errorLines := strings.Builder{}
		for i, match := range matches {
			if i == 0 {
				continue
			}
			numErrors++
			errorLines.WriteString(fmt.Sprintf("action '%s' references undefined 'args.%s'\n", actionName, strings.TrimSpace(match)))
		}

		// Each action must have a reward. TODO: Validate the reward is a valid expression.
		if reward, ok := rewards[actionName]; !ok {
			numErrors++
			errorLines.WriteString(fmt.Sprintf("reward for action '%s' is invalid '%s'\n", actionName, reward))
		}

		if numErrors > 0 {
			return errors.New(errorLines.String())
		}
	}

	return nil
}

func (pod *Pod) AddLocalState(newState ...*state.State) {
	pod.podLocalStateMutex.Lock()
	defer pod.podLocalStateMutex.Unlock()

	pod.podLocalState = append(pod.podLocalState, newState...)
}

func (pod *Pod) State() []*state.State {
	return pod.podLocalState
}

func (pod *Pod) LearningAlgorithm() string {
	return pod.podParams.LearningAlgorithm
}

func (pod *Pod) InitDataConnectors(handler state.StateHandler) error {
	pod.podLocalStateMutex.Lock()
	defer pod.podLocalStateMutex.Unlock()

	pod.podLocalStateHandlers = append(pod.podLocalStateHandlers, handler)

	errGroup, _ := errgroup.WithContext(context.Background())

	for _, ds := range pod.Dataspaces() {
		dsp := ds
		errGroup.Go(func() error {
			dsp.RegisterStateHandler(handler)
			return dsp.InitDataConnector(pod.podParams.Epoch, pod.podParams.Period, pod.podParams.Interval)
		})
	}

	return errGroup.Wait()
}

func unmarshalPod(podPath string) (*Pod, error) {
	podBytes, err := util.ReplaceEnvVariablesFromPath(podPath, constants.SpiceEnvVarPrefix)
	if err != nil {
		return nil, err
	}

	v := viper.New()
	v.SetConfigType("yaml")

	err = v.ReadConfig(bytes.NewBuffer(podBytes))
	if err != nil {
		return nil, err
	}

	var podSpec *spec.PodSpec

	err = v.Unmarshal(&podSpec)
	if err != nil {
		return nil, err
	}

	pod := &Pod{
		PodSpec:            *podSpec,
		viper:              v,
		podLocalStateMutex: sync.RWMutex{},
	}

	return pod, nil
}

func loadPod(podPath string, hash string) (*Pod, error) {
	pod, err := unmarshalPod(podPath)
	if err != nil {
		return nil, err
	}

	err = pod.loadParams()
	if err != nil {
		return nil, fmt.Errorf("error loading pod params: %s", err.Error())
	}

	pod.manifestPath = podPath
	pod.hash = hash
	if pod.Name == "" {
		pod.Name = strings.TrimSuffix(filepath.Base(podPath), filepath.Ext(podPath))
	}

	if pod.Time != nil && len(pod.Time.Categories) > 0 {
		pod.timeCategories = spice_time.GenerateTimeCategoryFields(pod.Time.Categories...)
		names := make([]string, len(pod.timeCategories))
		for name := range pod.timeCategories {
			names = append(names, name)
		}
		sort.Strings(names)
		pod.timeCategoryNames = names
	}

	pod.flights = make(map[string]*flights.Flight)

	var fqIdentifierNames []string
	var fqMeasurementNames []string
	var fqCategoryNames []string
	var tags []string

	tagsMap := make(map[string]bool)
	measurements := make(map[string]*dataspace.MeasurementInfo)
	dataspaceMap := make(map[string]*dataspace.Dataspace, len(pod.PodSpec.Dataspaces))

	for _, dsSpec := range pod.PodSpec.Dataspaces {
		ds, err := dataspace.NewDataspace(dsSpec)
		if err != nil {
			return nil, err
		}
		pod.dataspaces = append(pod.dataspaces, ds)
		dataspaceMap[ds.Path()] = ds

		for _, identifier := range ds.Identifiers() {
			fqIdentifierNames = append(fqIdentifierNames, identifier.FqName)
		}

		for fqMeasurementName, measurement := range ds.Measurements() {
			fqMeasurementNames = append(fqMeasurementNames, fqMeasurementName)
			measurements[fqMeasurementName] = measurement
		}

		for _, category := range ds.Categories() {
			fqCategoryNames = append(fqCategoryNames, category.FqName)
		}

		for _, dsTag := range ds.Tags() {
			if _, ok := tagsMap[dsTag]; !ok {
				tagsMap[dsTag] = true
				tags = append(tags, dsTag)
			}
		}
	}

	pod.dataspaceMap = dataspaceMap

	pod.actions = pod.getActions()

	sort.Strings(fqIdentifierNames)
	pod.fqIdentifierNames = fqIdentifierNames

	sort.Strings(fqMeasurementNames)
	pod.fqMeasurementNames = fqMeasurementNames
	pod.measurements = measurements

	sort.Strings(fqCategoryNames)
	pod.fqCategoryNames = fqCategoryNames

	sort.Strings(tags)
	pod.tags = tags

	pod.interpretations = interpretations.NewInterpretationsStore(pod.Epoch(), pod.Period(), pod.Granularity())

	if pod.Training != nil && pod.Training.RewardFuncs != "" {
		if !strings.HasSuffix(pod.Training.RewardFuncs, ".py") {
			return nil, errors.New("external reward functions must be defined in a single Python file - see https://docs.spiceai.org/concepts/rewards/")
		}

		rewardFuncBytes, err := os.ReadFile(pod.Training.RewardFuncs)
		if err != nil {
			return nil, err
		}

		pod.externalRewardFuncs = string(rewardFuncBytes)
	}

	return pod, err
}

func (pod *Pod) loadRewardSpecs() ([]spec.RewardSpec, error) {
	var rewards []spec.RewardSpec
	err := pod.viper.UnmarshalKey("training.rewards", &rewards)
	if err != nil {
		return nil, err
	}
	return rewards, nil
}

func (pod *Pod) IsSame(otherPod *Pod) bool {
	return otherPod != nil && pod.manifestPath == otherPod.manifestPath && pod.Hash() == otherPod.Hash()
}

func (pod *Pod) loadParams() error {
	podParams := NewPodParams()

	if pod.PodSpec.Params != nil {
		str, ok := pod.PodSpec.Params["epoch_time"]
		if ok {
			intVal, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				return err
			}
			podParams.Epoch = time.Unix(intVal, 0)
		}

		str, ok = pod.PodSpec.Params["period"]
		if ok {
			val, err := time.ParseDuration(str)
			if err != nil {
				return err
			}
			podParams.Period = val
		}

		str, ok = pod.PodSpec.Params["interval"]
		if ok {
			val, err := time.ParseDuration(str)
			if err != nil {
				return err
			}
			podParams.Interval = val
		}

		str, ok = pod.PodSpec.Params["granularity"]
		if ok {
			val, err := time.ParseDuration(str)
			if err != nil {
				return err
			}
			podParams.Granularity = val
		}
	}

	pod.podParams = podParams

	return nil
}

func (pod *Pod) getActions() map[string]string {
	allDataSourceActions := make(map[string]string)
	var dataSourcePrefixes []string
	for _, ds := range pod.Dataspaces() {
		for fqActionName, fqAction := range ds.Actions() {
			allDataSourceActions[fqActionName] = fqAction
			dataSourcePrefixes = append(dataSourcePrefixes, fmt.Sprintf("%s.%s", ds.DataspaceSpec.From, ds.DataspaceSpec.Name))
		}
	}

	globalActions := make(map[string]string)
	actions := make(map[string]string)

	for _, globalAction := range pod.PodSpec.Actions {
		if globalAction.Do == nil {
			actions[globalAction.Name] = ""
			continue
		}

		globalActions[globalAction.Name] = globalAction.Do.Name

		dsAction, ok := allDataSourceActions[globalAction.Do.Name]
		if !ok {
			actions[globalAction.Name] = globalAction.Do.Name
			continue
		}

		for argName, argValue := range globalAction.Do.Args {
			action := strings.ReplaceAll(dsAction, fmt.Sprintf("args.%s", argName), argValue)
			actions[globalAction.Name] = action
		}
	}

	// Hoist any dataspace actions to global scope if they don't exist
	for dsActionName, dsAction := range allDataSourceActions {
		dsActionExistsGlobally := false
		for globalActionName := range actions {
			if existingDsActionName, ok := globalActions[globalActionName]; ok {
				if dsActionName == existingDsActionName {
					dsActionExistsGlobally = true
					break
				}
			}
		}
		if !dsActionExistsGlobally {
			for _, prefix := range dataSourcePrefixes {
				if strings.HasPrefix(dsActionName, prefix) {
					actionName := strings.TrimPrefix(dsActionName, prefix+".")
					actions[actionName] = dsAction
					break
				}
			}
		}
	}
	return actions
}

func (pod *Pod) csvHeaders() string {
	if pod.fqCsvHeaders == "" {
		headers := make([]string, 0, len(pod.fqIdentifierNames)+len(pod.fqMeasurementNames)+len(pod.fqCategoryNames))
		headers = append(headers, pod.fqIdentifierNames...)
		headers = append(headers, pod.fqMeasurementNames...)
		headers = append(headers, pod.fqCategoryNames...)

		pod.fqCsvHeaders = fmt.Sprintf("time,%s,_tags\n", strings.Join(headers, ","))
	}

	return pod.fqCsvHeaders
}
