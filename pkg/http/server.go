package http

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/fasthttp/router"
	"github.com/spiceai/spiceai/pkg/aiengine"
	"github.com/spiceai/spiceai/pkg/api"
	"github.com/spiceai/spiceai/pkg/dashboard"
	"github.com/spiceai/spiceai/pkg/dataspace"
	"github.com/spiceai/spiceai/pkg/diagnostics"
	"github.com/spiceai/spiceai/pkg/environment"
	"github.com/spiceai/spiceai/pkg/flights"
	"github.com/spiceai/spiceai/pkg/loggers"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/proto/common_pb"
	"github.com/spiceai/spiceai/pkg/proto/runtime_pb"
	"github.com/spiceai/spiceai/pkg/state"
	spice_time "github.com/spiceai/spiceai/pkg/time"
	"github.com/valyala/fasthttp"
	"go.uber.org/zap"
)

type ServerConfig struct {
	Port uint
}

type server struct {
	config ServerConfig
}

var (
	zaplog *zap.Logger = loggers.ZapLogger()
)

func healthHandler(ctx *fasthttp.RequestCtx) {
	if !aiengine.ServerReady() {
		fmt.Fprintf(ctx, "ai engine initializing")
		return
	}

	err := aiengine.IsAIEngineHealthy()
	if err != nil {
		fmt.Fprintf(ctx, "degraded\n")
		fmt.Fprintf(ctx, "ai: %s", err.Error())
		return
	}

	if !environment.FirstInitializationCompleted() {
		fmt.Fprintf(ctx, "environment initializing")
		return
	}

	fmt.Fprintf(ctx, "ok")
}

func apiGetObservationsHandler(ctx *fasthttp.RequestCtx) {
	podParam := ctx.UserValue("pod").(string)
	pod := pods.GetPod(podParam)

	if pod == nil {
		ctx.Response.SetStatusCode(404)
		return
	}

	if string(ctx.Request.Header.Peek("Accept")) == "application/json" {
		observations := []*common_pb.Observation{}
		for _, state := range pod.CachedState() {
			obs := api.NewObservationsFromState(state)
			observations = append(observations, obs...)
		}
		ctx.Response.Header.Add("Content-Type", "application/json")
		data, err := json.Marshal(observations)
		if err != nil {
			ctx.Response.SetStatusCode(500)
			fmt.Fprintf(ctx, "error getting observations: %s", err.Error())
			return
		}
		_, _ = ctx.Write(data)
		return
	}

	ctx.Response.Header.Add("Content-Type", "text/csv")
	csv := pod.CachedCsv()
	_, _ = ctx.WriteString(csv)
}

func apiPostObservationsHandler(ctx *fasthttp.RequestCtx) {
	podParam := ctx.UserValue("pod").(string)
	pod := pods.GetPod(podParam)

	if pod == nil {
		ctx.Response.SetStatusCode(404)
		return
	}

	validMeasurementNames := pod.MeasurementNames()

	newState, err := state.GetStateFromCsv(validMeasurementNames, ctx.Request.Body())
	if err != nil {
		ctx.Response.SetStatusCode(400)
		fmt.Fprintf(ctx, "error processing csv: %s", err.Error())
		return
	}

	pod.AddLocalState(newState...)

	ctx.Response.SetStatusCode(201)
}

func apiPostDataspaceHandler(ctx *fasthttp.RequestCtx) {
	podParam := ctx.UserValue("pod").(string)
	pod := pods.GetPod(podParam)

	if pod == nil {
		ctx.Response.SetStatusCode(404)
		return
	}

	dataspaceFrom := ctx.UserValue("dataspace_from").(string)
	dataspaceName := ctx.UserValue("dataspace_name").(string)

	var selectedDataspace *dataspace.Dataspace
	for _, dataspace := range pod.DataSpaces() {
		if dataspace.DataspaceSpec.From == dataspaceFrom && dataspace.DataspaceSpec.Name == dataspaceName {
			selectedDataspace = dataspace
			break
		}
	}

	if selectedDataspace == nil {
		ctx.Response.SetStatusCode(http.StatusNotFound)
		return
	}

	_, err := selectedDataspace.ReadData(ctx.Request.Body(), nil)
	if err != nil {
		zaplog.Sugar().Error(err)
		ctx.Response.SetStatusCode(500)
		return
	}

	ctx.Response.SetStatusCode(201)
}

func apiGetPodsHandler(ctx *fasthttp.RequestCtx) {
	pods := pods.Pods()

	data := make([]*runtime_pb.Pod, 0, len(pods))

	for _, f := range pods {
		if f == nil {
			continue
		}

		item := api.NewPod(f)
		data = append(data, item)
	}

	response, err := json.Marshal(data)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		ctx.Response.SetBodyString(err.Error())
		return
	}

	ctx.Response.Header.SetContentType("application/json")
	ctx.Response.SetBody(response)
}

func apiGetPodHandler(ctx *fasthttp.RequestCtx) {
	podParam := ctx.UserValue("pod").(string)
	pod := pods.GetPod(podParam)

	if pod == nil {
		ctx.Response.SetStatusCode(404)
		return
	}

	data := api.NewPod(pod)

	response, err := json.Marshal(data)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		ctx.Response.SetBodyString(err.Error())
		return
	}

	ctx.Response.Header.SetContentType("application/json")
	ctx.Response.SetBody(response)
}

func apiPodTrainHandler(ctx *fasthttp.RequestCtx) {
	podParam := ctx.UserValue("pod").(string)
	pod := pods.GetPod(podParam)

	if pod == nil {
		ctx.Response.SetStatusCode(404)
		return
	}

	var trainRequest runtime_pb.TrainModel
	err := json.Unmarshal(ctx.Request.Body(), &trainRequest)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		ctx.Response.SetBodyString(err.Error())
		return
	}

	err = aiengine.StartTraining(pod, trainRequest.LearningAlgorithm, trainRequest.NumberEpisodes)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		ctx.Response.SetBodyString(err.Error())
		return
	}

	fmt.Fprintf(ctx, "ok")
}

func apiRecommendationHandler(ctx *fasthttp.RequestCtx) {
	pod := ctx.UserValue("pod").(string)
	tag := ctx.UserValue("tag")

	if tag == nil || tag == "" {
		tag = "latest"
	}

	inference, err := aiengine.Infer(pod, tag.(string))
	if err != nil {
		ctx.Response.SetStatusCode(500)
		ctx.Response.SetBodyString(err.Error())
		return
	}

	if inference.Response.Error {
		ctx.Response.SetStatusCode(400)
	}

	body, err := json.Marshal(inference)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		ctx.Response.SetBodyString(err.Error())
		return
	}

	ctx.Response.SetBody(body)
}

func apiGetFlightsHandler(ctx *fasthttp.RequestCtx) {
	podParam := ctx.UserValue("pod").(string)
	pod := pods.GetPod(podParam)
	if pod == nil {
		ctx.Response.SetStatusCode(404)
		return
	}

	data := make([]*runtime_pb.Flight, 0)
	for _, f := range *pod.Flights() {
		flight := api.NewFlight(f)
		data = append(data, flight)
	}

	response, err := json.Marshal(data)
	if err != nil {
		ctx.Response.Header.SetContentType("application/json")
		return
	}

	ctx.Response.SetBody(response)
}

func apiGetFlightHandler(ctx *fasthttp.RequestCtx) {
	podParam := ctx.UserValue("pod").(string)
	pod := pods.GetPod(podParam)
	if pod == nil {
		ctx.Response.SetStatusCode(404)
		return
	}

	flightParam := ctx.UserValue("flight").(string)
	flight := pod.GetFlight(flightParam)
	if flight == nil {
		ctx.Response.SetStatusCode(404)
		return
	}

	data := api.NewFlight(flight)

	response, err := json.Marshal(data)
	if err != nil {
		ctx.Response.Header.SetContentType("application/json")
		return
	}

	ctx.Response.SetBody(response)
}

func apiPostFlightEpisodeHandler(ctx *fasthttp.RequestCtx) {
	podParam := ctx.UserValue("pod").(string)
	pod := pods.GetPod(podParam)
	if pod == nil {
		ctx.Response.SetStatusCode(404)
		return
	}

	flightParam := ctx.UserValue("flight").(string)
	flight := pod.GetFlight(flightParam)
	if flight == nil {
		ctx.Response.SetStatusCode(404)
		return
	}

	var apiEpisode runtime_pb.Episode
	err := json.Unmarshal(ctx.Request.Body(), &apiEpisode)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		ctx.Response.SetBodyString(err.Error())
		return
	}

	episode := &flights.Episode{
		EpisodeId:    apiEpisode.Episode,
		Start:        time.Unix(apiEpisode.Start, 0),
		End:          time.Unix(apiEpisode.End, 0),
		Score:        apiEpisode.Score,
		ActionsTaken: apiEpisode.ActionsTaken,
		Error:        apiEpisode.Error,
		ErrorMessage: apiEpisode.ErrorMessage,
	}

	flight.RecordEpisode(episode)

	ctx.Response.SetStatusCode(201)
}

func apiGetInterpretationsHandler(ctx *fasthttp.RequestCtx) {
	podParam := ctx.UserValue("pod").(string)
	pod := pods.GetPod(podParam)
	if pod == nil {
		ctx.Response.SetStatusCode(http.StatusNotFound)
		return
	}

	start := pod.Epoch()
	startArg := ctx.QueryArgs().Peek("start")
	if startArg != nil {
		start, err := spice_time.ParseTime(string(startArg), "")
		if err != nil {
			ctx.Response.SetStatusCode(http.StatusBadRequest)
			ctx.Response.SetBodyString(fmt.Sprintf("invalid start %s", startArg))
			return
		}

		if start.Before(pod.Epoch()) {
			ctx.Response.SetStatusCode(http.StatusBadRequest)
			ctx.Response.SetBodyString(fmt.Sprintf("start %s cannot be before pod epoch %s", startArg, pod.Epoch().String()))
			return
		}
	}

	podPeriodEnd := pod.Epoch().Add(pod.Period())
	end := podPeriodEnd
	endArg := ctx.QueryArgs().Peek("end")
	if endArg != nil {
		end, err := spice_time.ParseTime(string(endArg), "")
		if err != nil {
			ctx.Response.SetStatusCode(http.StatusBadRequest)
			ctx.Response.SetBodyString(fmt.Sprintf("invalid end %s", endArg))
			return
		}

		if end.After(podPeriodEnd) {
			ctx.Response.SetStatusCode(http.StatusBadRequest)
			ctx.Response.SetBodyString(fmt.Sprintf("end %s cannot be after pod period %s", startArg, podPeriodEnd.String()))
			return
		}
	}

	if end.Before(start) {
		ctx.Response.SetStatusCode(http.StatusBadRequest)
		ctx.Response.SetBodyString(fmt.Sprintf("end %s cannot be before start %s", endArg, startArg))
		return
	}

	interpretations := pod.Interpretations().Get(start, end)
	apiInterpretations := api.ApiInterpretations(interpretations)

	response, err := json.Marshal(apiInterpretations)
	if err != nil {
		ctx.Response.Header.SetContentType("application/json")
		return
	}

	ctx.Response.SetBody(response)
}

func apiPostInterpretationsHandler(ctx *fasthttp.RequestCtx) {
	podParam := ctx.UserValue("pod").(string)
	pod := pods.GetPod(podParam)
	if pod == nil {
		ctx.Response.SetStatusCode(http.StatusNotFound)
		return
	}

	var apiInterpretations []*api.Interpretation
	err := json.Unmarshal(ctx.Request.Body(), &apiInterpretations)
	if err != nil {
		ctx.Response.SetStatusCode(http.StatusBadRequest)
		ctx.Response.SetBodyString(err.Error())
		return
	}

	for _, i := range apiInterpretations {
		interpretation, err := api.NewInterpretationFromApi(i)
		if err != nil {
			ctx.Response.SetStatusCode(http.StatusBadRequest)
			ctx.Response.SetBodyString(err.Error())
			return
		}

		err = pod.Interpretations().Add(interpretation)
		if err != nil {
			ctx.Response.SetStatusCode(http.StatusBadRequest)
			ctx.Response.SetBodyString(err.Error())
			return
		}
	}

	ctx.Response.SetStatusCode(http.StatusCreated)
}

func apiPostExportHandler(ctx *fasthttp.RequestCtx) {
	tag := ctx.UserValue("tag")

	if tag == nil || tag == "" {
		tag = "latest"
	}

	podParam := ctx.UserValue("pod").(string)
	pod := pods.GetPod(podParam)
	if pod == nil {
		ctx.Response.SetStatusCode(404)
		return
	}

	var exportRequest runtime_pb.ExportModel
	err := json.Unmarshal(ctx.Request.Body(), &exportRequest)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		ctx.Response.SetBodyString(err.Error())
		return
	}

	err = aiengine.ExportPod(pod.Name, tag.(string), &exportRequest)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		ctx.Response.SetBodyString(err.Error())
		return
	}

	ctx.Response.SetStatusCode(200)
}

func apiPostImportHandler(ctx *fasthttp.RequestCtx) {
	tag := ctx.UserValue("tag")

	if tag == nil || tag == "" {
		tag = "latest"
	}

	podName := ctx.UserValue("pod").(string)

	var importRequest runtime_pb.ImportModel
	err := json.Unmarshal(ctx.Request.Body(), &importRequest)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		ctx.Response.SetBodyString(err.Error())
		return
	}

	importRequest.Pod = podName
	importRequest.Tag = tag.(string)

	pod, err := pods.ImportPod(importRequest.Pod, importRequest.ArchivePath)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		ctx.Response.SetBodyString(err.Error())
		return
	}

	err = aiengine.ImportPod(pod, &importRequest)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		ctx.Response.SetBodyString(err.Error())
		return
	}

	err = environment.InitPodDataConnector(pod)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		ctx.Response.SetBodyString(err.Error())
		return
	}

	ctx.Response.SetStatusCode(200)
}

func (server *server) apiGetDiagnosticsHandler(ctx *fasthttp.RequestCtx) {
	report, err := diagnostics.GenerateReport()
	if err != nil {
		ctx.Response.SetStatusCode(500)
		ctx.Response.SetBodyString(err.Error())
		return
	}

	ctx.SetBodyString(report)
}

func NewServer(port uint) *server {
	return &server{
		config: ServerConfig{
			Port: port,
		},
	}
}

func (server *server) Start() error {
	r := router.New()
	r.GET("/health", healthHandler)

	// Static Dashboard
	dashboardServer := dashboard.NewDashboardEmbedded()
	var err error

	api := r.Group("/api/v0.1")
	{
		// Pods
		api.GET("/pods", apiGetPodsHandler)
		api.GET("/pods/{pod}", apiGetPodHandler)
		api.POST("/pods/{pod}/train", apiPodTrainHandler)
		api.GET("/pods/{pod}/observations", apiGetObservationsHandler)
		api.POST("/pods/{pod}/observations", apiPostObservationsHandler)
		api.GET("/pods/{pod}/recommendation", apiRecommendationHandler)
		api.GET("/pods/{pod}/models/{tag}/recommendation", apiRecommendationHandler)
		api.POST("/pods/{pod}/export", apiPostExportHandler)
		api.POST("/pods/{pod}/models/{tag}/export", apiPostExportHandler)
		api.POST("/pods/{pod}/import", apiPostImportHandler)
		api.POST("/pods/{pod}/models/{tag}/import", apiPostImportHandler)
		api.POST("/pods/{pod}/dataspaces/{dataspace_from}/{dataspace_name}/data", apiPostDataspaceHandler)

		// Flights
		api.GET("/pods/{pod}/training_runs", apiGetFlightsHandler)
		api.GET("/pods/{pod}/training_runs/{flight}", apiGetFlightHandler)
		api.POST("/pods/{pod}/training_runs/{flight}/episodes", apiPostFlightEpisodeHandler)

		// Interpretations
		api.GET("/pods/{pod}/interpretations", apiGetInterpretationsHandler)
		api.POST("/pods/{pod}/interpretations", apiPostInterpretationsHandler)

		api.GET("/diagnostics", server.apiGetDiagnosticsHandler)
	}

	static := r.Group("/static")
	{
		static.GET("/js/{file}", dashboardServer.JsHandler)
		static.GET("/css/{file}", dashboardServer.CssHandler)
		static.GET("/media/{file}", dashboardServer.MediaHandler)
	}

	r.GET("/manifest.json", dashboardServer.ManifestJsonHandler)
	r.GET("/{filepath:*}", func(ctx *fasthttp.RequestCtx) {
		if strings.Contains(ctx.URI().String(), "/api/") {
			ctx.Response.SetStatusCode(http.StatusNotFound)
			return
		}
		dashboardServer.IndexHandler(ctx)
	})
	r.GET("/", dashboardServer.IndexHandler)

	serverLogger, err := zap.NewStdLogAt(zaplog, zap.DebugLevel)
	if err != nil {
		return fmt.Errorf("failed to initialize logger: %w", err)
	}
	fastServer := &fasthttp.Server{
		Handler: r.Handler,
		Logger:  serverLogger,
	}

	go func() {
		log.Fatal(fastServer.ListenAndServe(fmt.Sprintf(":%d", server.config.Port)))
	}()

	return nil
}
