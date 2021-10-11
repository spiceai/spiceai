package aiengine

import (
	"path/filepath"
	"testing"

	"github.com/spiceai/spiceai/pkg/pods"
)

func TestGetPodInitForTraining(t *testing.T) {
	manifestsToTest := []string{"event-tags.yaml", "trader.yaml", "trader-infer.yaml", "event-tags.yaml"}

	for _, manifestToTest := range manifestsToTest {
		manifestPath := filepath.Join("../../test/assets/pods/manifests", manifestToTest)

		pod, err := pods.LoadPodFromManifest(manifestPath)
		if err != nil {
			t.Error(err)
			return
		}

		if pod.PodSpec.Params == nil {
			pod.PodSpec.Params = make(map[string]string)
		}

		initRequest := getPodInitForTraining(pod)
		initRequest.EpochTime = 123456789
		err = snapshotter.SnapshotMulti(filepath.Base(pod.ManifestPath()), initRequest)
		if err != nil {
			t.Fatal(err)
		}
	}
}