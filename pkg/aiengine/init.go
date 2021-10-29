package aiengine

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/proto/aiengine_pb"
)

var podInitMap map[string]*aiengine_pb.InitRequest

func InitializePod(pod *pods.Pod) error {
	err := pod.ValidateForTraining()
	if err != nil {
		return err
	}

	podInit := getPodInitForTraining(pod)

	err = sendInit(podInit)
	if err != nil {
		return err
	}

	podInitMap[pod.Name] = podInit

	return nil
}

func sendInit(podInit *aiengine_pb.InitRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := aiengineClient.Init(ctx, podInit)
	if err != nil {
		return err
	}

	if response.Error {
		return fmt.Errorf("failed to validate manifest: %s", response.Result)
	}

	return nil
}

func init() {
	podInitMap = make(map[string]*aiengine_pb.InitRequest)
}

func getPodInitForTraining(pod *pods.Pod) *aiengine_pb.InitRequest {
	fields := make(map[string]*aiengine_pb.FieldData)

	globalActions := pod.Actions()
	globalFieldsWithArgs := append(pod.MeasurementNames(), pod.ActionsArgs()...)
	var laws []string

	var dsInitSpecs []*aiengine_pb.DataSource
	for _, ds := range pod.Dataspaces() {
		for fqField, measurement := range ds.Measurements() {
			measurementName := strings.ReplaceAll(fqField, ".", "_")
			measurementData := &aiengine_pb.FieldData{
				Initializer: measurement.InitialValue,
				FillMethod:  aiengine_pb.FillType_FILL_FORWARD,
			}

			if measurement.Fill == "none" {
				measurementData.FillMethod = aiengine_pb.FillType_FILL_ZERO
			}

			fields[measurementName] = measurementData
		}

		for _, category := range ds.Categories() {
			for _, oneHotFieldName := range category.EncodedFieldNames {
				fields[oneHotFieldName] = &aiengine_pb.FieldData{
					Initializer: 0.0,
					FillMethod:  aiengine_pb.FillType_FILL_ZERO,
				}
			}
		}

		for _, localTag := range ds.Tags() {
			fqTag := fmt.Sprintf("%s_%s_%s", ds.From, ds.DataspaceSpec.Name, localTag)
			fields[fqTag] = &aiengine_pb.FieldData{
				Initializer: 0.0,
				FillMethod:  aiengine_pb.FillType_FILL_ZERO,
			}
		}

		dsActions := make(map[string]string)
		for dsAction := range ds.DataspaceSpec.Actions {
			fqAction, ok := globalActions[dsAction]
			if ok {
				dsActions[dsAction] = replaceDotNotatedFieldNames(fqAction, globalFieldsWithArgs)
			}
		}

		for _, law := range ds.Laws() {
			laws = append(laws, replaceDotNotatedFieldNames(law, globalFieldsWithArgs))
		}

		dsInitSpec := aiengine_pb.DataSource{
			Actions: dsActions,
		}
		if ds.DataspaceSpec.Data != nil {
			dsInitSpec.Connector = &aiengine_pb.DataConnector{
				Name:   ds.DataspaceSpec.Data.Connector.Name,
				Params: ds.DataspaceSpec.Data.Connector.Params,
			}
		} else {
			dsInitSpec.Connector = &aiengine_pb.DataConnector{
				Name: "localstate",
			}
		}

		dsInitSpecs = append(dsInitSpecs, &dsInitSpec)
	}

	var rewardInit *string
	if pod.PodSpec.Training != nil {
		rewardInitTrimmed := strings.TrimSpace(pod.PodSpec.Training.RewardInit)
		if rewardInitTrimmed != "" {
			rewardInit = &rewardInitTrimmed
		}
	}

	externalRewardFuncs, _ := pod.ExternalRewardFuncs()

	actionRewards := pod.Rewards()
	actionsOrder := make(map[string]int32, len(globalActions))
	var actionNames []string

	// If the rewards are defined inline, then process them into globalActionRewards
	if externalRewardFuncs == "" {
		rewards := pod.Rewards()
		globalActionRewards := make(map[string]string)
		for actionName := range globalActions {
			globalActionRewards[actionName] = rewards[actionName]
			actionNames = append(actionNames, actionName)
			if rewardInit != nil {
				reward := *rewardInit + "\n" + rewards[actionName]
				reward = replaceDotNotatedFieldNames(reward, globalFieldsWithArgs)
				globalActionRewards[actionName] = reward
			}
		}

		actionRewards = globalActionRewards
	}

	sort.Strings(actionNames)
	for order, action := range actionNames {
		actionsOrder[action] = int32(order)
	}

	podInit := aiengine_pb.InitRequest{
		Pod:                 pod.Name,
		EpochTime:           pod.Epoch().Unix(),
		Period:              int64(pod.Period().Seconds()),
		Interval:            int64(pod.Interval().Seconds()),
		Granularity:         int64(pod.Granularity().Seconds()),
		Datasources:         dsInitSpecs,
		Fields:              fields,
		Actions:             actionRewards,
		ActionsOrder:        actionsOrder,
		Laws:                laws,
		ExternalRewardFuncs: externalRewardFuncs,
	}

	return &podInit
}

func replaceDotNotatedFieldNames(content string, fieldNames []string) string {
	newContent := content
	for _, fieldName := range fieldNames {
		newContent = strings.ReplaceAll(newContent, fieldName, strings.ReplaceAll(fieldName, ".", "_"))
	}
	return newContent
}
