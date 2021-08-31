package registry_test

import (
	"testing"

	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/registry"
	"github.com/spiceai/spiceai/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func TestRegistry(t *testing.T) {
	testutils.EnsureTestSpiceDirectory(t)
	t.Run("testGetPod() -- Local registry should fetch pod", testGetPod())
	t.Cleanup(testutils.CleanupTestSpiceDirectory)
}

func testGetPod() func(*testing.T) {
	return func(t *testing.T) {
		manifestPath := "../../test/assets/pods/manifests/trader.yaml"
		r := registry.GetRegistry(manifestPath)
		_, err := r.GetPod(manifestPath)
		assert.NoError(t, err)

		pod, err := pods.LoadPodFromManifest(".spice/pods/trader.yaml")
		if assert.NoError(t, err) {
			assert.Contains(t, pod.Name, "trader")
		}
	}
}
