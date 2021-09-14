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
	"time"

	"github.com/spiceai/spiceai/pkg/api"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/proto/aiengine_pb"
	"github.com/spiceai/spiceai/pkg/proto/runtime_pb"
	"github.com/spiceai/spiceai/pkg/tempdir"
	"github.com/spiceai/spiceai/pkg/util"
	"google.golang.org/protobuf/proto"
)

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

	interpretations := pod.Interpretations()
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

func ImportPod(request *runtime_pb.ImportModel) error {
	if !ServerReady() {
		return fmt.Errorf("not ready")
	}

	tempDir, err := tempdir.CreateTempDir("import")
	if err != nil {
		return err
	}
	err = util.ExtractZipFileToDir(request.ArchivePath, tempDir)
	if err != nil {
		return err
	}

	var init aiengine_pb.InitRequest
	initBytes, err := os.ReadFile(filepath.Join(tempDir, "init.pb"))
	if err != nil {
		return err
	}
	err = proto.Unmarshal(initBytes, &init)
	if err != nil {
		return err
	}

	manifestPath := filepath.Join(tempDir, fmt.Sprintf("%s.yaml", request.Pod))
	pod, err := pods.LoadPodFromManifest(manifestPath)
	if err != nil {
		return err
	}

	pods.CreateOrUpdatePod(pod)

	err = sendInit(&init)
	if err != nil {
		return err
	}

	interpretationsPath := filepath.Join(tempDir, "interpretations.json")
	if _, err := os.Stat(interpretationsPath); err == nil {
		interpretationsBytes, err := os.ReadFile(filepath.Join(tempDir, "interpretations.json"))
		if err != nil {
			return err
		}
		var apiInterpretations []api.Interpretation
		err = json.Unmarshal(interpretationsBytes, &apiInterpretations)
		if err != nil {
			return err
		}
		for _, i := range apiInterpretations {
			interpretation, err := api.NewInterpretationFromApi(&i)
			if err != nil {
				return err
			}
			err = pod.AddInterpretation(interpretation)
			if err != nil {
				return err
			}
		}
	}

	podState, err := pod.FetchNewData()
	if err != nil {
		return err
	}

	err = SendData(pod, podState...)
	if err != nil {
		return err
	}

	modelName := fmt.Sprintf("%s.model", init.Pod)
	modelPath := filepath.Join(tempDir, modelName)

	importRequest := &aiengine_pb.ImportModelRequest{
		Pod:        request.Pod,
		Tag:        request.Tag,
		ImportPath: modelPath,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := aiengineClient.ImportModel(ctx, importRequest)
	if err != nil {
		return err
	}

	if response.Error {
		return fmt.Errorf("%s: %s", response.Result, response.Message)
	}

	return nil
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
