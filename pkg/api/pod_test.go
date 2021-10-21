package api

import (
	"testing"

	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/testutils"
)

func TestNewPod(t *testing.T) {
	snapshotter := testutils.NewSnapshotter("../../test/assets/snapshots/api/pod")

	pod, err := pods.LoadPodFromManifest("../../test/assets/pods/manifests/event-categories.yaml")
	if err != nil {
		t.Fatal(err)
	}

	apiPod := NewPod(pod)

	snapshotter.SnapshotTJson(t, apiPod)
}
