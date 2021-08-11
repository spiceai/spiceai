package http

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/fasthttp/router"
	"github.com/logrusorgru/aurora"
	"github.com/spiceai/spice/pkg/aiengine"
	"github.com/spiceai/spice/pkg/api"
	"github.com/spiceai/spice/pkg/csv"
	"github.com/spiceai/spice/pkg/dashboard"
	"github.com/spiceai/spice/pkg/flights"
	"github.com/spiceai/spice/pkg/pods"
	"github.com/valyala/fasthttp"
)

type ServerConfig struct {
	Port uint
}

type server struct {
	config ServerConfig
}

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

	reader := bytes.NewReader(ctx.Request.Body())
	validFieldNames := pod.FieldNames()
	newState, err := csv.ProcessCsvByPath(reader, &validFieldNames)
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

	data := make([]*api.Pod, 0)

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

func apiInferHandler(ctx *fasthttp.RequestCtx) {
	pod := ctx.UserValue("pod").(string)
	tag := ctx.UserValue("tag")

	if tag == nil || tag == "" {
		tag = "latest"
	}

	body, err := aiengine.Infer(pod, tag.(string))
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

	data := make([]*api.Flight, 0)
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

	var apiEpisode api.Episode
	err := json.Unmarshal(ctx.Request.Body(), &apiEpisode)
	if err != nil {
		ctx.Response.SetStatusCode(400)
		ctx.Response.SetBody([]byte(err.Error()))
		return
	}

	episode := &flights.Episode{
		EpisodeId:    apiEpisode.EpisodeId,
		Start:        time.Unix(apiEpisode.Start, 0),
		End:          time.Unix(apiEpisode.End, 0),
		Score:        apiEpisode.Score,
		ActionsTaken: apiEpisode.ActionsTaken,
		Error:        apiEpisode.Error,
		ErrorMessage: apiEpisode.ErrorMessage,
	}

	flight.RecordEpisode(episode)

	if episode.Error != "" {
		log.Printf("Training stopped -> %s: %s\n", aurora.Red(episode.Error), aurora.Yellow(episode.ErrorMessage))
	}

	ctx.Response.SetStatusCode(201)
}

func NewServer(port uint) *server {
	return &server{
		config: ServerConfig{
			Port: port,
		},
	}
}

func (server *server) Start() {
	r := router.New()
	r.GET("/health", healthHandler)

	// Static Dashboard
	r.GET("/", dashboard.DashboardIndexHandler)
	r.GET("/js/{jsFile}", dashboard.DashboardAppHandler)

	// Pods
	r.GET("/api/v0.1/pods", apiPodsHandler)
	r.GET("/api/v0.1/pods/{pod}", apiPodHandler)
	r.POST("/api/v0.1/pods/{pod}/train", apiPodTrainHandler)
	r.GET("/api/v0.1/pods/{pod}/observations", apiGetObservationsHandler)
	r.POST("/api/v0.1/pods/{pod}/observations", apiPostObservationsHandler)
	r.GET("/api/v0.1/pods/{pod}/inference", apiInferHandler)
	r.GET("/api/v0.1/pods/{pod}/models/{tag}/inference", apiInferHandler)

	// Flights
	r.GET("/api/v0.1/pods/{pod}/flights", apiGetFlightsHandler)
	r.GET("/api/v0.1/pods/{pod}/flights/{flight}", apiGetFlightHandler)
	r.POST("/api/v0.1/pods/{pod}/flights/{flight}/episodes", apiPostFlightEpisodeHandler)

	go func() {
		log.Fatal(fasthttp.ListenAndServe(fmt.Sprintf(":%d", server.config.Port), r.Handler))
	}()
}
