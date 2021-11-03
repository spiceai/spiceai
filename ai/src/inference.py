from pathlib import Path
from typing import Dict

import json
import pandas as pd

from algorithms.factory import get_agent
from algorithms.agent_interface import SpiceAIAgent
from data import DataManager
from exception import InvalidDataShapeException
from proto.aiengine.v1 import aiengine_pb2
from train import Trainer


class GetInferenceHandler:
    def __init__(self, request: aiengine_pb2.InferenceRequest, data_managers: Dict[str, DataManager]):
        self.request = request
        self.data_managers = data_managers
        self.inference_time = pd.to_datetime(request.inference_time, unit="s")
        self.use_latest_time = request.inference_time == 0

    def __is_valid_inference_time(self, first_valid_time, last_valid_time):
        if self.request.inference_time == 0:
            return True
        return self.request.inference_time >= first_valid_time and self.request.inference_time <= last_valid_time

    def __validate_request(self) -> aiengine_pb2.InferenceResult:
        if self.request.pod not in self.data_managers:
            return aiengine_pb2.InferenceResult(
                response=aiengine_pb2.Response(result="pod_not_initialized", error=True))

        if self.request.tag != "latest":
            return aiengine_pb2.InferenceResult(
                response=aiengine_pb2.Response(
                    result="tag_not_yet_supported", message="Support for multiple tags coming soon!", error=True))

        data_manager = self.data_managers[self.request.pod]

        first_valid_time = (data_manager.massive_table_filled.first_valid_index() +
                            data_manager.param.interval_secs).timestamp()
        last_valid_time = data_manager.massive_table_filled.last_valid_index().timestamp()

        if not self.__is_valid_inference_time(first_valid_time, last_valid_time):
            result = "invalid_recommendation_time"
            message = f"The time specified ({self.request.inference_time}) "\
                      f"is outside of the allowed range: ({int(first_valid_time)}, {int(last_valid_time)})"
            return aiengine_pb2.InferenceResult(
                response=aiengine_pb2.Response(
                    result=result, message=message, error=True
                )
            )

        return None

    def __load_agent(self, model_exists) -> SpiceAIAgent:
        # Ideally we could just re-use the in-memory agent we created during training,
        # but tensorflow has issues with multi-threading in python, so we are just loading it from the file system
        data_manager = self.data_managers[self.request.pod]
        model_data_shape = data_manager.get_shape()
        if model_exists:
            save_path = Trainer.SAVED_MODELS[self.request.pod]
            with open(save_path / "meta.json", "r", encoding="utf-8") as meta_file:
                save_info = json.loads(meta_file.read())
            algorithm = save_info["algorithm"]
        else:
            algorithm = "dql"

        agent: SpiceAIAgent = get_agent(algorithm, model_data_shape, len(data_manager.action_names))
        if model_exists:
            agent.load(Path(save_path))
        return agent

    def get_result(self):
        try:
            model_exists = False
            if self.request.pod in Trainer.SAVED_MODELS:
                model_exists = True

            error = self.__validate_request()
            if error is not None:
                return error

            agent = self.__load_agent(model_exists=model_exists)

            data_manager = self.data_managers[self.request.pod]

            if data_manager.massive_table_filled.shape[0] < data_manager.get_window_span():
                return aiengine_pb2.InferenceResult(
                    response=aiengine_pb2.Response(result="not_enough_data", error=True))

            if self.use_latest_time:
                latest_window = data_manager.get_latest_window()
                state = data_manager.flatten_and_normalize_window(latest_window)
            else:
                requested_window = data_manager.get_window_at(self.inference_time)
                state = data_manager.flatten_and_normalize_window(requested_window)

            action_from_model, probabilities = agent.act(state)
        except InvalidDataShapeException as ex:
            result = ex.type
            message = ex.message

            if Trainer.TRAINING_LOCK.locked():
                result = "training_not_complete"
                message = (
                    "Please wait to get a recommendation until after training completes"
                )

            return aiengine_pb2.InferenceResult(
                response=aiengine_pb2.Response(
                    result=result, message=message, error=True
                )
            )
        confidence = f"{probabilities[action_from_model]:.3f}"
        if not model_exists:
            confidence = 0.0

        action_name = data_manager.action_names[action_from_model]

        if self.use_latest_time:
            end_time = data_manager.massive_table_filled.last_valid_index()
        else:
            end_time = self.inference_time

        start_time = end_time - data_manager.param.interval_secs

        result = aiengine_pb2.InferenceResult()
        result.start = int(start_time.timestamp())
        result.end = int(end_time.timestamp())
        result.action = action_name
        result.confidence = float(confidence)
        result.tag = self.request.tag
        result.response.result = "ok"

        return result
