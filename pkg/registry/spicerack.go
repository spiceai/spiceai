package registry

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"github.com/spiceai/spice/pkg/config"
	"github.com/spiceai/spice/pkg/loggers"
	"go.uber.org/zap"
)

const (
	spiceRackBaseUrl string = "http://prod.spicerack.org/api/v0.1" // Will update to https://api.spicerack.org
)

var (
	zaplog *zap.Logger = loggers.ZapLogger()
)

type SpiceRackRegistry struct{}

func (r *SpiceRackRegistry) GetPod(podPath string) (string, error) {
	podName := strings.ToLower(filepath.Base(podPath))
	podManifestFileName := fmt.Sprintf("%s.yaml", podName)

	url := fmt.Sprintf("%s/pods/%s", spiceRackBaseUrl, podPath)
	failureMessage := fmt.Sprintf("An error occurred while fetching pod '%s' from spicerack.org", podPath)

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
		return "", fmt.Errorf("an error occurred fetching pod %s", podPath)
	}

	downloadPath := filepath.Join(config.PodsManifestsPath(), podManifestFileName)

	err = os.MkdirAll(config.PodsManifestsPath(), 0766)
	if err != nil {
		return "", err
	}

	err = os.WriteFile(downloadPath, body, 0666)
	if err != nil {
		return "", fmt.Errorf("an error occurred downloading pod %s", podPath)
	}

	return downloadPath, nil
}
