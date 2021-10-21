package aiengine

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/logrusorgru/aurora"
	"github.com/spiceai/spiceai/pkg/flights"
	"github.com/spiceai/spiceai/pkg/pods"
	"github.com/spiceai/spiceai/pkg/proto/aiengine_pb"
)

func StartTraining(pod *pods.Pod, algorithm string, number_episodes int64) error {
	flightId := fmt.Sprintf("%d", len(*pod.Flights())+1)

	flight := flights.NewFlight(flightId, int(pod.Episodes()))

	// Once we have an AI engine -> spiced gRPC channel, this should be done on demand
	err := sendInterpretations(pod, pod.Interpretations().IndexedInterpretations())
	if err != nil {
		return err
	}

	trainRequest := &aiengine_pb.StartTrainingRequest{
		Pod:               pod.Name,
		EpochTime:         pod.Epoch().Unix(),
		Flight:            flightId,
		NumberEpisodes:    int64(flight.ExpectedEpisodes()),
		TrainingGoal:      pod.PodSpec.Training.Goal,
		LearningAlgorithm: pod.LearningAlgorithm(),
	}
	// Overload pod's parameters
	if algorithm != "" {
		trainRequest.LearningAlgorithm = algorithm
	}
	if number_episodes > 0 {
		trainRequest.NumberEpisodes = number_episodes
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	response, err := aiengineClient.StartTraining(ctx, trainRequest)
	if err != nil {
		return fmt.Errorf("%s -> failed to verify training has started: %w", pod.Name, err)
	}

	switch response.Result {
	case "already_training":
		return fmt.Errorf("%s -> training is already in progress", pod.Name)
	case "not_enough_data_for_training":
		return fmt.Errorf("%s -> insufficient data for training", pod.Name)
	case "epoch_time_invalid":
		return fmt.Errorf("%s -> epoch time %d invalid: %s", pod.Name, pod.Epoch().Unix(), response.Message)
	case "started_training":
		pod.AddFlight(flightId, flight)
		log.Println(fmt.Sprintf("%s -> %s", pod.Name, aurora.BrightCyan("Starting training...")))
	default:
		return fmt.Errorf("%s -> failed to verify training has started: %s", pod.Name, response.Result)
	}

	if !aiSingleTrainingRun {
		return nil
	}

	<-*flight.WaitForDoneChan()

	return nil
}
