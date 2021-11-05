package aiengine

import (
	"archive/zip"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/spiceai/spiceai/pkg/api"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/proto/aiengine_pb"
	"github.com/spiceai/spiceai/pkg/proto/runtime_pb"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
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
		if ds.DataspaceSpec.Data == nil {
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

	externalRewardFuncs := pod.ExternalRewardFuncs()

	actionRewards := pod.Rewards()
	actionsOrder := make(map[string]int32, len(globalActions))
	var actionNames []string

	globalActionRewards := make(map[string]string)
	for actionName := range globalActions {
		globalActionRewards[actionName] = actionRewards[actionName]
		actionNames = append(actionNames, actionName)
		if rewardInit != nil {
			reward := *rewardInit + "\n" + actionRewards[actionName]
			reward = replaceDotNotatedFieldNames(reward, globalFieldsWithArgs)
			globalActionRewards[actionName] = reward
		}
	}

	if externalRewardFuncs == "" {
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

func ExportPod(podName string, tag string, request *runtime_pb.ExportModel) error {
	if !ServerReady() {
		return fmt.Errorf("not ready")
	}

	aiRequest := &aiengine_pb.ExportModelRequest{
		Pod: podName,
		Tag: tag,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	result, err := aiengineClient.ExportModel(ctx, aiRequest)
	if err != nil {
		return err
	}

	if result.Response.Error {
		return fmt.Errorf("%s: %s", result.Response.Result, result.Response.Message)
	}

	var absDir string = filepath.Dir(result.ModelPath)
	var files []string = make([]string, 0, 10)
	err = filepath.WalkDir(result.ModelPath, func(path string, info fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		f, err := filepath.Rel(absDir, path)
		if err != nil {
			return err
		}
		files = append(files, f)
		return nil
	})
	if err != nil {
		return err
	}

	fullPath := filepath.Join(request.Directory, request.Filename)
	modelArchive, err := os.Create(fullPath)
	if err != nil {
		return err
	}
	defer modelArchive.Close()

	zipWriter := zip.NewWriter(modelArchive)
	defer zipWriter.Close()

	for _, f := range files {
		err = addFileOrDirToZip(zipWriter, filepath.Join(absDir, f), f)
		if err != nil {
			return err
		}
	}

	init := podInitMap[podName]
	initBytes, err := proto.Marshal(init)
	if err != nil {
		return err
	}

	pod := pods.GetPod(podName)
	if pod == nil {
		return fmt.Errorf("pod not loaded")
	}

	manifestBytes, err := os.ReadFile(pod.ManifestPath())
	if err != nil {
		return err
	}

	interpretations := pod.Interpretations().All()
	apiInterpretations := api.ApiInterpretations(interpretations)
	interpretationData, err := json.Marshal(apiInterpretations)
	if err != nil {
		return err
	}

	err = addBytesAsFileToZip(zipWriter, interpretationData, "interpretations.json")
	if err != nil {
		return err
	}

	err = addBytesAsFileToZip(zipWriter, initBytes, "init.pb")
	if err != nil {
		return err
	}
	err = addBytesAsFileToZip(zipWriter, manifestBytes, fmt.Sprintf("%s.yaml", podName))
	if err != nil {
		return err
	}

	return nil
}

func ImportPod(pod *pods.Pod, request *runtime_pb.ImportModel) error {
	if !ServerReady() {
		return fmt.Errorf("not ready")
	}

	podDir := filepath.Dir(pod.ManifestPath())

	var init aiengine_pb.InitRequest
	initBytes, err := os.ReadFile(filepath.Join(podDir, "init.pb"))
	if err != nil {
		return err
	}
	err = proto.Unmarshal(initBytes, &init)
	if err != nil {
		return err
	}

	err = sendInit(&init)
	if err != nil {
		return err
	}

	errGroup, _ := errgroup.WithContext(context.Background())

	errGroup.Go(func() error {
		interpretationsPath := filepath.Join(podDir, "interpretations.json")
		return importInterpretations(pod, interpretationsPath)
	})

	errGroup.Go(func() error {
		podState := pod.CachedState()
		return SendData(pod, podState...)
	})

	errGroup.Go(func() error {
		return importModel(pod, request.Tag)
	})

	return errGroup.Wait()
}

func addBytesAsFileToZip(zipWriter *zip.Writer, fileContent []byte, filename string) error {
	header := &zip.FileHeader{
		Name:   filename,
		Method: zip.Store,
	}
	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		return err
	}

	_, err = io.Copy(writer, bytes.NewReader(fileContent))
	if err != nil {
		return err
	}

	return nil
}

func addFileOrDirToZip(zipWriter *zip.Writer, fullPath string, relativePath string) error {
	fileToZip, err := os.Open(fullPath)
	if err != nil {
		return err
	}
	defer fileToZip.Close()

	info, err := fileToZip.Stat()
	if err != nil {
		return err
	}

	if info.IsDir() {
		// To create a directory instead of a file, add a trailing slash to the name.
		_, err = zipWriter.Create(fmt.Sprintf("%s/", relativePath))
		if err != nil {
			return err
		}
		return nil
	}

	header, err := zip.FileInfoHeader(info)
	if err != nil {
		return err
	}

	// Using FileInfoHeader() above only uses the basename of the file. If we want
	// to preserve the folder structure we can overwrite this with the full path.
	header.Name = relativePath
	header.Method = zip.Store

	writer, err := zipWriter.CreateHeader(header)
	if err != nil {
		return err
	}
	_, err = io.Copy(writer, fileToZip)
	return err
}
