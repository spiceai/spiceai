package http

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/fasthttp/router"
	"github.com/spiceai/data-components-contrib/dataprocessors"
	"github.com/spiceai/data-components-contrib/dataprocessors/csv"
	"github.com/spiceai/spiceai/pkg/aiengine"
	"github.com/spiceai/spiceai/pkg/api"
	"github.com/spiceai/spiceai/pkg/dashboard"
	"github.com/spiceai/spiceai/pkg/flights"
	"github.com/spiceai/spiceai/pkg/loggers"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/proto/runtime_pb"
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
		fmt.Fprintf(ctx, "initializing")
		return
	}

	err := aiengine.IsServerHealthy()
	if err != nil {
		fmt.Fprintf(ctx, "degraded\n")
		fmt.Fprintf(ctx, "ai: %s", err.Error())
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

	csv := pod.CachedCsv()

	ctx.Response.Header.Add("Content-Type", " text/csv")
	_, _ = ctx.Write([]byte(csv))
}

func apiPostObservationsHandler(ctx *fasthttp.RequestCtx) {
	podParam := ctx.UserValue("pod").(string)
	pod := pods.GetPod(podParam)

	if pod == nil {
		ctx.Response.SetStatusCode(404)
		return
	}

	dp, err := dataprocessors.NewDataProcessor(csv.CsvProcessorName)
	if err != nil {
		zaplog.Sugar().Error(err)
		ctx.Response.SetStatusCode(500)
	}

	_, err = dp.OnData(ctx.Request.Body())
	if err != nil {
		zaplog.Sugar().Error(err)
		ctx.Response.SetStatusCode(500)
	}

	validFieldNames := pod.FieldNames()

	newState, err := dp.GetState(&validFieldNames)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		fmt.Fprintf(ctx, "error processing csv: %s", err.Error())
		return
	}

	pod.AddLocalState(newState...)

	ctx.Response.SetStatusCode(201)
}

func apiPodsHandler(ctx *fasthttp.RequestCtx) {
	pods := pods.Pods()

	data := make([]*runtime_pb.Pod, 0)

	for _, f := range *pods {
		if f == nil {
			continue
		}

		item := api.NewPod(f)
		data = append(data, item)
	}

	response, err := json.Marshal(data)
	if err != nil {
		ctx.Response.Header.SetContentType("application/json")
		return
	}

	ctx.Response.SetBody(response)
}

func apiPodHandler(ctx *fasthttp.RequestCtx) {
	podParam := ctx.UserValue("pod").(string)
	pod := pods.GetPod(podParam)

	if pod == nil {
		ctx.Response.SetStatusCode(404)
		return
	}

	data := api.NewPod(pod)

	response, err := json.Marshal(data)
	if err != nil {
		ctx.Response.Header.SetContentType("application/json")
		return
	}

	ctx.Response.SetBody(response)
}

func apiPodTrainHandler(ctx *fasthttp.RequestCtx) {
	podParam := ctx.UserValue("pod").(string)
	pod := pods.GetPod(podParam)

	if pod == nil {
		ctx.Response.SetStatusCode(404)
		return
	}

	err := aiengine.StartTraining(pod)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		ctx.Response.SetBody([]byte(err.Error()))
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
		ctx.Response.SetBody([]byte(err.Error()))
		return
	}

	if inference.Response.Error {
		ctx.Response.SetStatusCode(400)
	}

	body, err := json.Marshal(inference)
	if err != nil {
		ctx.Response.SetStatusCode(500)
		ctx.Response.SetBody([]byte(err.Error()))
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
		ctx.Response.SetBody([]byte(err.Error()))
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
		ctx.Response.SetBody([]byte(err.Error()))
		return
	}

	err = aiengine.ExportModel(pod.Name, tag.(string), &exportRequest)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		ctx.Response.SetBody([]byte(err.Error()))
		return
	}

	ctx.Response.SetStatusCode(200)
}

func apiPostImportHandler(ctx *fasthttp.RequestCtx) {
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

	var importRequest runtime_pb.ImportModel
	err := json.Unmarshal(ctx.Request.Body(), &importRequest)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		ctx.Response.SetBody([]byte(err.Error()))
		return
	}

	importRequest.Pod = pod.Name
	importRequest.Tag = tag.(string)

	err = aiengine.ImportModel(&importRequest)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		ctx.Response.SetBody([]byte(err.Error()))
		return
	}

	ctx.Response.SetStatusCode(200)
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

	r.GET("/", dashboardServer.IndexHandler)
	r.GET("/manifest.json", dashboardServer.ManifestJsonHandler)
	r.GET("/acknowledgements", dashboardServer.AcknowledgementsHandler)
	r.GET("/static/js/{file}", dashboardServer.JsHandler)
	r.GET("/static/css/{file}", dashboardServer.CssHandler)
	r.GET("/static/media/{file}", dashboardServer.SvgHandler)

	// Pods
	r.GET("/api/v0.1/pods", apiPodsHandler)
	r.GET("/api/v0.1/pods/{pod}", apiPodHandler)
	r.POST("/api/v0.1/pods/{pod}/train", apiPodTrainHandler)
	r.GET("/api/v0.1/pods/{pod}/observations", apiGetObservationsHandler)
	r.POST("/api/v0.1/pods/{pod}/observations", apiPostObservationsHandler)
	r.GET("/api/v0.1/pods/{pod}/recommendation", apiRecommendationHandler)
	r.GET("/api/v0.1/pods/{pod}/models/{tag}/recommendation", apiRecommendationHandler)
	r.POST("/api/v0.1/pods/{pod}/export", apiPostExportHandler)
	r.POST("/api/v0.1/pods/{pod}/models/{tag}/export", apiPostExportHandler)
	r.POST("/api/v0.1/pods/{pod}/import", apiPostImportHandler)
	r.POST("/api/v0.1/pods/{pod}/models/{tag}/import", apiPostImportHandler)

	// Flights
	r.GET("/api/v0.1/pods/{pod}/training_runs", apiGetFlightsHandler)
	r.GET("/api/v0.1/pods/{pod}/training_runs/{flight}", apiGetFlightHandler)
	r.POST("/api/v0.1/pods/{pod}/training_runs/{flight}/episodes", apiPostFlightEpisodeHandler)

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
