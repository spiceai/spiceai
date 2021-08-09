package environment_test

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/spiceai/spice/pkg/aiengine"
	"github.com/spiceai/spice/pkg/environment"
	"github.com/spiceai/spice/pkg/pods"
	"github.com/spiceai/spice/pkg/testutils"
	"github.com/stretchr/testify/assert"
)

func TestEnvironment(t *testing.T) {
	origHttpClient := aiengine.HttpClient
	t.Cleanup(func() { aiengine.HttpClient = origHttpClient })

	t.Run("testStartDataListeners() -- Should start listeners and post data", testStartDataListeners())
}

func testStartDataListeners() func(*testing.T) {
	return func(t *testing.T) {
		pod, err := pods.LoadPodFromManifest("../../test/assets/pods/manifests/trader.yaml")
		assert.NoError(t, err)
		pods.CreateOrUpdatePod(pod)
		data_received := make(chan bool)

		aiengine.HttpClient = testutils.NewTestClient(func(req *http.Request) (*http.Response, error) {
			switch req.URL.String() {
			case "http://localhost:8004/health":

				return &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(bytes.NewBufferString("ok")),
					Header:     make(http.Header),
				}, nil

			case "http://localhost:8004/pods/trader/data":
				data_received <- true
				return &http.Response{
					StatusCode: 200,
					Body:       io.NopCloser(bytes.NewBufferString("ok")),
					Header:     make(http.Header),
				}, nil

			}

			return nil, fmt.Errorf("Unexpected request: %s", req.URL.String())
		})

		go func() {
			err = environment.StartDataListeners(1)
			assert.NoError(t, err)
		}()

		go func() {
			time.Sleep(3000 * time.Millisecond)
			fmt.Println("expired")
			data_received <- false
		}()

		assert.True(t, <-data_received)
	}
}
