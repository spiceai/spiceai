package http

import (
	"encoding/json"
	"testing"

	"github.com/spiceai/spiceai/pkg/api"
	"github.com/spiceai/spiceai/pkg/interpretations"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
)

func TestServer(t *testing.T) {
	manifestPath := "../../test/assets/pods/manifests/trader.yaml"

	pod, err := pods.LoadPodFromManifest(manifestPath)
	if err != nil {
		t.Error(err)
		return
	}

	t.Run("getInterpretations()", testGetInterpretationsHandlerFunc(pod))
	t.Run("postInterpretations()", testPostInterpretationsHandlerFunc(pod))
}

func testGetInterpretationsHandlerFunc(pod *pods.Pod) func(t *testing.T) {
	return func(t *testing.T) {
		interpretation, err := interpretations.NewInterpretation(
			pod.Epoch(),
			pod.Epoch().Add(pod.Interval()),
			"test interpretation",
		)
		if err != nil {
			t.Error(err)
		}

		err = pod.AddInterpretation(interpretation)
		if err != nil {
			t.Error(err)
		}

		ctx := &fasthttp.RequestCtx{
			Request: fasthttp.Request{},
		}
		ctx.SetUserValue("pod", "trader")

		apiGetInterpretationsHandler(ctx)

		interpretations := pod.Interpretations()
		assert.Equal(t, 1, len(interpretations))
		assert.Equal(t, interpretation, &interpretations[0])
	}
}

func testPostInterpretationsHandlerFunc(pod *pods.Pod) func(t *testing.T) {
	return func(t *testing.T) {
		interpretation, err := interpretations.NewInterpretation(
			pod.Epoch(),
			pod.Epoch().Add(pod.Interval()),
			"test interpretation",
		)
		if err != nil {
			t.Error(err)
		}

		ctx := &fasthttp.RequestCtx{
			Request: fasthttp.Request{},
		}
		ctx.SetUserValue("pod", "trader")

		apiInterpretation := api.NewApiInterpretation(interpretation)

		data, err := json.Marshal(apiInterpretation)
		if err != nil {
			t.Error(err)
		}

		ctx.Request.SetBody(data)

		apiPostInterpretationsHandler(ctx)

		interpretations := pod.Interpretations()
		assert.Equal(t, 1, len(interpretations))
		assert.Equal(t, interpretation, &interpretations[0])
	}
}
