package pods

import (
	"bytes"
	"errors"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/spf13/viper"
	"github.com/spiceai/spice/pkg/config"
	"github.com/spiceai/spice/pkg/datasources"
	"github.com/spiceai/spice/pkg/flights"
	"github.com/spiceai/spice/pkg/models"
	"github.com/spiceai/spice/pkg/observations"
	"github.com/spiceai/spice/pkg/spec"
	"github.com/spiceai/spice/pkg/state"
	"github.com/spiceai/spice/pkg/util"
)

type Pod struct {
	spec.PodSpec
	hash               string
	manifestPath       string
	dataSources        []*datasources.DataSource
	fields             map[string]float64
	fieldNames         []string
	flights            map[string]*flights.Flight
	viper              *viper.Viper
	podLocalState      []*state.State
	podLocalStateMutex sync.RWMutex
}

func (pod *Pod) Hash() string {
	return pod.hash
}

func (f *Pod) ManifestPath() string {
	return f.manifestPath
}

func (pod *Pod) Period() time.Duration {
	if pod.PodSpec.Params != nil {
		str, ok := pod.PodSpec.Params["period"]
		if ok {
			val, err := time.ParseDuration(str)
			if err == nil {
				return val
			}
		}
	}

	return time.Hour * 24 * 3 // Default to 3 days
}

func (pod *Pod) Interval() time.Duration {
	if pod.PodSpec.Params != nil {
		str, ok := pod.PodSpec.Params["interval"]
		if ok {
			val, err := time.ParseDuration(str)
			if err == nil {
				return val
			}
		}
	}

	return time.Minute * 1 // Default to 1 min
}

func (pod *Pod) Epoch() time.Time {
	if pod.PodSpec.Params != nil {
		str, ok := pod.PodSpec.Params["epoch_time"]
		if ok {
			intVal, err := strconv.ParseInt(str, 10, 64)
			if err == nil {
				return time.Unix(intVal, 0)
			}
		}
	}

	return time.Now().Add(-pod.Period())
}

func (pod *Pod) Granularity() time.Duration {
	if pod.PodSpec.Params != nil {
		str, ok := pod.PodSpec.Params["granularity"]
		if ok {
			val, err := time.ParseDuration(str)
			if err == nil {
				return val
			}
		}
	}

	return time.Second * 10 // Default to 10 sec
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
			if episodes, err := strconv.ParseInt(episodesParam, 0, 64); err == nil {
				return int(episodes)
			}
		}
	}
	return 10
}

func (pod *Pod) CachedState() []*state.State {
	var cachedState []*state.State
	for _, ds := range pod.DataSources() {
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
	fieldNames := pod.FieldNames()

	st := fmt.Sprintf("time,%s\n", strings.Join(fieldNames, ","))
	csv.WriteString(st)

	cachedState := pod.CachedState()
	for _, state := range cachedState {
		var validFieldNames []string

		for _, globalFieldName := range fieldNames {
			isValid := false
			for _, fieldName := range state.FieldNames() {
				field := state.Path() + "." + fieldName
				if globalFieldName == field {
					validFieldNames = append(validFieldNames, fieldName)
					isValid = true
					break
				}
			}
			if !isValid {
				validFieldNames = append(validFieldNames, globalFieldName)
			}
		}

		stateCsv, _ := observations.GetCsv(validFieldNames, state.Observations(), 0)
		csv.WriteString(stateCsv)
	}
	return csv.String()
}

func (pod *Pod) DataSources() []*datasources.DataSource {
	return pod.dataSources
}

func (pod *Pod) Flights() *map[string]*flights.Flight {
	return &pod.flights
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
	allDataSourceActions := make(map[string]string)
	var dataSourcePrefixes []string
	for _, ds := range pod.DataSources() {
		for fqActionName, fqAction := range ds.Actions() {
			allDataSourceActions[fqActionName] = fqAction
			dataSourcePrefixes = append(dataSourcePrefixes, fmt.Sprintf("%s.%s", ds.DataSourceSpec.From, ds.DataSourceSpec.Name))
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

	// Hoist any datasource actions to global scope if they don't exist
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

func (pod *Pod) Fields() map[string]float64 {
	return pod.fields
}

func (pod *Pod) FieldNames() []string {
	return pod.fieldNames
}

func (pod *Pod) ValidateForTraining() error {
	if pod.Granularity() > pod.Interval() {
		return errors.New("granularity must be less than or equal to interval")
	}

	if pod.Interval() > pod.Period() {
		return errors.New("interval must be less than or equal to period")
	}

	if pod.PodSpec.DataSources == nil || len(pod.PodSpec.DataSources) < 1 {
		return errors.New("at least one datasource is required for training")
	}

	actions := pod.Actions()

	if len(actions) == 0 {
		return errors.New("at least one action is required for training")
	}

	// Check for args.<arg name>
	re := regexp.MustCompile("[=| ]args\\.(\\w+)[=| \n]")

	rewards := pod.Rewards()

	for actionName, action := range actions {
		numErrors := 0
		matches := re.FindStringSubmatch(action)
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

func (pod *Pod) State() ([]*state.State, error) {
	var allState []*state.State
	for _, ds := range pod.DataSources() {
		state, err := ds.FetchNewState(pod.Epoch(), pod.Period(), pod.Interval())
		if err != nil {
			return nil, err
		}
		if state == nil {
			continue
		}
		allState = append(allState, state...)
	}

	allState = append(allState, pod.podLocalState...)

	return allState, nil
}

func (pod *Pod) FetchNewData() ([]*state.State, error) {
	var allState []*state.State
	for _, ds := range pod.DataSources() {
		state, err := ds.FetchNewState(pod.Epoch(), pod.Period(), pod.Interval())
		if err != nil {
			return nil, err
		}
		allState = append(allState, state...)
	}
	return allState, nil
}

func (pod *Pod) DownloadModelUpdate(connectionId string, connection config.ConnectionSpec, branch string) (string, error) {
	downloader, err := models.GetDownloader(pod.Name, connectionId, connection, branch)
	if err != nil {
		return "", err
	}

	return downloader.Fetch()
}

func unmarshalPod(podPath string) (*Pod, error) {
	podBytes, err := util.ReplaceEnvVariablesFromPath(podPath, config.SpiceEnvVarPrefix)
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

	pod.manifestPath = podPath
	pod.hash = hash
	if pod.Name == "" {
		pod.Name = strings.TrimSuffix(filepath.Base(podPath), filepath.Ext(podPath))
	}

	pod.flights = make(map[string]*flights.Flight)

	var fieldNames []string
	fields := make(map[string]float64)

	for _, dsSpec := range pod.PodSpec.DataSources {
		ds, err := datasources.NewDataSource(dsSpec)
		if err != nil {
			return nil, err
		}
		pod.dataSources = append(pod.dataSources, ds)

		for _, fieldName := range ds.FieldNameMap() {
			fieldNames = append(fieldNames, fieldName)
		}

		for field, intializer := range ds.Fields() {
			fields[field] = intializer
		}
	}

	sort.Strings(fieldNames)
	pod.fieldNames = fieldNames
	pod.fields = fields

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