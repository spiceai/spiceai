package registry_test

import (
	"os"
	"testing"

	"github.com/spiceai/spiceai/pkg/constants"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/registry"
	"github.com/spiceai/spiceai/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func TestLocalFileRegistry(t *testing.T) {
	testutils.EnsureTestSpiceDirectory(t)
	t.Run("testGetPod() -- Local registry should fetch pod", testGetPod())
	t.Cleanup(testutils.CleanupTestSpiceDirectory)
}

func testGetPod() func(*testing.T) {
	return func(t *testing.T) {
		manifestPath := "../../test/assets/pods/trader"
		r := registry.GetRegistry(manifestPath)
		_, err := r.GetPod(manifestPath)
		assert.NoError(t, err)
		defer os.RemoveAll(constants.SpicePodsDirectoryName)

		_, err = os.Stat("spicepods/data")
		assert.NoError(t, err)

		_, err = os.Stat("spicepods/data/fake-data.json")
		assert.NoError(t, err)

		pod, err := pods.LoadPodFromManifest("spicepods/trader.yaml")
		if assert.NoError(t, err) {
			assert.Contains(t, pod.Name, "trader")
		}
	}
}
