package registry

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/spiceai/spiceai/pkg/context"
	"github.com/spiceai/spiceai/pkg/loggers"
	"go.uber.org/zap"
)

const (
	spiceRackBaseUrl string = "https://api.spicerack.org/api/v0.1"
)

var (
	zaplog *zap.Logger = loggers.ZapLogger()
)

type SpiceRackRegistry struct{}

func (r *SpiceRackRegistry) GetPod(podFullPath string) (string, error) {
	parts := strings.Split(podFullPath, "@")
	podPath := podFullPath
	podVersion := ""
	if len(parts) == 2 {
		podPath = parts[0]
		podVersion = parts[1]
	}

	podName := strings.ToLower(filepath.Base(podPath))
	podManifestFileName := fmt.Sprintf("%s.yaml", podName)

	url := fmt.Sprintf("%s/pods/%s", spiceRackBaseUrl, podPath)
	if podVersion != "" {
		url = fmt.Sprintf("%s/%s", url, podVersion)
	}
	failureMessage := fmt.Sprintf("An error occurred while fetching pod '%s' from spicerack.org", podFullPath)

	response, err := http.Get(url)
	if err != nil {
		zaplog.Sugar().Debugf("%s: %s", failureMessage, err.Error())
		return "", errors.New(failureMessage)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		zaplog.Sugar().Debugf("%s: %s", failureMessage, err.Error())
		return "", errors.New(failureMessage)
	}

	if response.StatusCode == 404 {
		return "", NewRegistryItemNotFound(fmt.Errorf("pod %s not found", podPath))
	}

	if response.StatusCode != 200 {
		return "", fmt.Errorf("an error occurred fetching pod '%s'", podPath)
	}

	podsPath := context.CurrentContext().PodsDir()
	downloadPath := filepath.Join(podsPath, podManifestFileName)

	err = os.MkdirAll(podsPath, 0766)
	if err != nil {
		return "", err
	}

	err = os.WriteFile(downloadPath, body, 0666)
	if err != nil {
		return "", fmt.Errorf("an error occurred downloading pod %s", podPath)
	}

	return downloadPath, nil
}
