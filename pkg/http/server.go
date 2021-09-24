package http

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/fasthttp/router"
	"github.com/spiceai/data-components-contrib/dataprocessors"
	"github.com/spiceai/data-components-contrib/dataprocessors/csv"
	"github.com/spiceai/spiceai/pkg/aiengine"
	"github.com/spiceai/spiceai/pkg/api"
	"github.com/spiceai/spiceai/pkg/dashboard"
	"github.com/spiceai/spiceai/pkg/dataspaces"
	"github.com/spiceai/spiceai/pkg/flights"
	"github.com/spiceai/spiceai/pkg/loggers"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/proto/runtime_pb"
	"github.com/spiceai/spiceai/pkg/state"
	"github.com/spiceai/spiceai/pkg/util"
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
	_, _ = ctx.WriteString(csv)
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

	newState, err := dp.GetState(validFieldNames)
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

	var selectedDataspace *dataspaces.Dataspace
	for _, dataspace := range pod.DataSources() {
		if dataspace.DataspaceSpec.From == dataspaceFrom && dataspace.DataspaceSpec.Name == dataspaceName {
			selectedDataspace = dataspace
		}
	}

	if selectedDataspace == nil {
		ctx.Response.SetStatusCode(http.StatusNotFound)
		return
	}

	dp, err := dataprocessors.NewDataProcessor(selectedDataspace.DataspaceSpec.Data.Processor.Name)
	if err != nil {
		zaplog.Sugar().Error(err)
		ctx.Response.SetStatusCode(500)
		return
	}

	err = dp.Init(nil)
	if err != nil {
		zaplog.Sugar().Error(err)
		ctx.Response.SetStatusCode(500)
		return
	}

	_, err = dp.OnData(ctx.Request.Body())
	if err != nil {
		zaplog.Sugar().Error(err)
		ctx.Response.SetStatusCode(500)
		return
	}

	observations, err := dp.GetObservations()
	if err != nil {
		zaplog.Sugar().Error(err)
		ctx.Response.SetStatusCode(500)
		return
	}

	newState := state.NewState(selectedDataspace.Path(), selectedDataspace.FieldNames(), selectedDataspace.Tags(), observations)
	selectedDataspace.AddNewState(newState)

	ctx.Response.SetStatusCode(201)
}

func apiPodsHandler(ctx *fasthttp.RequestCtx) {
	pods := pods.Pods()

	data := make([]*runtime_pb.Pod, 0)

	for _, f := range pods {
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
		startTime, err := util.ParseTime(string(startArg))
		if err != nil {
			ctx.Response.SetStatusCode(http.StatusBadRequest)
			ctx.Response.SetBodyString(fmt.Sprintf("invalid start %s", startArg))
			return
		}
		start = time.Unix(startTime, 0)

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
		endTime, err := util.ParseTime(string(endArg))
		if err != nil {
			ctx.Response.SetStatusCode(http.StatusBadRequest)
			ctx.Response.SetBodyString(fmt.Sprintf("invalid end %s", endArg))
			return
		}
		end = time.Unix(endTime, 0)

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
		ctx.Response.SetBodyString(err.Error())
		return
	}

	importRequest.Pod = pod.Name
	importRequest.Tag = tag.(string)

	err = aiengine.ImportPod(&importRequest)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		ctx.Response.SetBodyString(err.Error())
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

	api := r.Group("/api/v0.1")
	{
		// Pods
		api.GET("/pods", apiPodsHandler)
		api.GET("/pods/{pod}", apiPodHandler)
		api.POST("/pods/{pod}/train", apiPodTrainHandler)
		api.GET("/pods/{pod}/observations", apiGetObservationsHandler)
		api.POST("/pods/{pod}/observations", apiPostObservationsHandler)
		api.GET("/pods/{pod}/recommendation", apiRecommendationHandler)
		api.GET("/pods/{pod}/models/{tag}/recommendation", apiRecommendationHandler)
		api.POST("/pods/{pod}/export", apiPostExportHandler)
		api.POST("/pods/{pod}/models/{tag}/export", apiPostExportHandler)
		api.POST("/pods/{pod}/import", apiPostImportHandler)
		api.POST("/pods/{pod}/models/{tag}/import", apiPostImportHandler)
		api.POST("/pods/{pod}/dataspaces/{dataspace_from}/{dataspace_name}", apiPostDataspaceHandler)

		// Flights
		api.GET("/pods/{pod}/training_runs", apiGetFlightsHandler)
		api.GET("/pods/{pod}/training_runs/{flight}", apiGetFlightHandler)
		api.POST("/pods/{pod}/training_runs/{flight}/episodes", apiPostFlightEpisodeHandler)

		// Interpretations
		api.GET("/pods/{pod}/interpretations", apiGetInterpretationsHandler)
		api.POST("/pods/{pod}/interpretations", apiPostInterpretationsHandler)
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
