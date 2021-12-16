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
	"github.com/spiceai/spiceai/pkg/proto/runtime_pb"
)

func StartTraining(pod *pods.Pod, trainModel *runtime_pb.TrainModel) error {
	if trainModel == nil {
		// Use pod defaults
		trainModel = &runtime_pb.TrainModel{
			LearningAlgorithm: pod.LearningAlgorithm(),
			NumberEpisodes:    pod.Episodes(),
			Loggers:           pod.TrainingLoggers(),
		}
	}

	if trainModel.NumberEpisodes <= 0 {
		trainModel.NumberEpisodes = int64(pod.Episodes())
	}

	algorithmId := trainModel.LearningAlgorithm
	if algorithmId == "" {
		algorithmId = pod.LearningAlgorithm()
	}

	algorithm := GetAlgorithm(algorithmId)
	if algorithm == nil {
		return fmt.Errorf("Learning algorithm %s not found", algorithmId)
	}

	if len(trainModel.Loggers) == 0 {
		trainModel.Loggers = pod.TrainingLoggers()
	}

	// Once we have an AI engine -> spiced gRPC channel, this should be done on demand
	err := sendInterpretations(pod, pod.Interpretations().IndexedInterpretations())
	if err != nil {
		return err
	}

	flightId := fmt.Sprintf("%d", len(*pod.Flights())+1)
	flight, err := flights.NewFlight(flightId, trainModel.NumberEpisodes, algorithm.Id, trainModel.Loggers)
	if err != nil {
		return err
	}

	for _, loggerId := range trainModel.Loggers {
		logger, err := flight.LoadLogger(loggerId)
		if err != nil {
			return err
		}
		fmt.Printf("%s -> Using training logger %s\n", pod.Name, logger.Name())
	}

	trainRequest := &aiengine_pb.StartTrainingRequest{
		Pod:               pod.Name,
		EpochTime:         pod.Epoch().Unix(),
		Flight:            flightId,
		NumberEpisodes:    int64(flight.ExpectedEpisodes()),
		TrainingGoal:      pod.PodSpec.Training.Goal,
		LearningAlgorithm: algorithm.Id,
		TrainingLoggers:   trainModel.Loggers,
		TrainingDataDir:   flight.DataDir(),
	}

	// Overload pod's parameters
	if trainModel.NumberEpisodes > 0 {
		trainRequest.NumberEpisodes = trainModel.NumberEpisodes
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
