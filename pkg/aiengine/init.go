package aiengine

import (
	"context"
	"fmt"
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

	podInit.ActionsOrder = make(map[string]int32, len(podInit.Actions))

	var order int32 = 0
	for action := range podInit.Actions {
		podInit.ActionsOrder[action] = order
		order += 1
	}

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

		for fqCategoryName, category := range ds.Categories() {
			for _, categoryValue := range category.Values {
				oneHotFieldName := fmt.Sprintf("%s-%s", fqCategoryName, categoryValue)
				oneHotFieldName = strings.ReplaceAll(oneHotFieldName, ".", "_")
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

	rewards := pod.Rewards()
	globalActionRewards := make(map[string]string)
	for actionName := range globalActions {
		globalActionRewards[actionName] = rewards[actionName]
		if rewardInit != nil {
			reward := *rewardInit + "\n" + rewards[actionName]
			reward = replaceDotNotatedFieldNames(reward, globalFieldsWithArgs)
			globalActionRewards[actionName] = reward
		}
	}

	epoch := pod.Epoch().Unix()

	podInit := aiengine_pb.InitRequest{
		Pod:         pod.Name,
		EpochTime:   epoch,
		Period:      int64(pod.Period().Seconds()),
		Interval:    int64(pod.Interval().Seconds()),
		Granularity: int64(pod.Granularity().Seconds()),
		Datasources: dsInitSpecs,
		Fields:      fields,
		Actions:     globalActionRewards,
		Laws:        laws,
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
