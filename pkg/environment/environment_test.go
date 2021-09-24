package environment_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/spiceai/spiceai/pkg/aiengine"
	"github.com/spiceai/spiceai/pkg/environment"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/proto/aiengine_pb"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestEnvironment(t *testing.T) {
	t.Run("RegisterStateHandlers() -- Should register handlers and post data", testRegisterStateHandlers())
}

func testRegisterStateHandlers() func(*testing.T) {
	return func(t *testing.T) {
		data_received := make(chan bool)
		aiengine.SetAIEngineClient(&aiengine.MockAIEngineClient{
			GetHealthHandler: func(c context.Context, hr *aiengine_pb.HealthRequest, co ...grpc.CallOption) (*aiengine_pb.Response, error) {
				return &aiengine_pb.Response{
					Result: "ok",
				}, nil
			},
			AddDataHandler: func(c context.Context, adr *aiengine_pb.AddDataRequest, co ...grpc.CallOption) (*aiengine_pb.Response, error) {
				data_received <- true
				return &aiengine_pb.Response{
					Result: "ok",
				}, nil
			},
			AddInterpretationsHandler: func(c context.Context, adr *aiengine_pb.AddInterpretationsRequest, co ...grpc.CallOption) (*aiengine_pb.Response, error) {
				data_received <- true
				return &aiengine_pb.Response{
					Result: "ok",
				}, nil
			},
		})

		pod, err := pods.LoadPodFromManifest("../../test/assets/pods/manifests/trader.yaml")
		assert.NoError(t, err)
		pods.CreateOrUpdatePod(pod)

		t.Cleanup(func() {
			aiengine.SetAIEngineClient(nil)
		})

		go func() {
			err := environment.InitDataConnectors()
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
