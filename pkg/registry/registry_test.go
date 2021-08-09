package registry_test

import (
	"testing"

	"github.com/spiceai/spice/pkg/pods"
	"github.com/spiceai/spice/pkg/registry"
	"github.com/spiceai/spice/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func TestRegistry(t *testing.T) {
	testutils.EnsureTestSpiceDirectory(t)
	t.Run("testGetPod() -- Local registry should fetch pod", testGetPod())
	t.Cleanup(testutils.CleanupTestSpiceDirectory)
}

func testGetPod() func(*testing.T) {
	return func(t *testing.T) {
		r := registry.GetRegistry("file://../../test/assets/pods/manifests/trader.yaml")
		_, err := r.GetPod("../../test/assets/pods/manifests/trader.yaml")
		assert.NoError(t, err)

		pod, err := pods.LoadPodFromManifest(".spice/pods/trader.yaml")
		assert.NoError(t, err)
		assert.Contains(t, pod.Name, "trader")
	}
}
