from concurrent import futures
from io import StringIO
import json
import os
from pathlib import Path
import signal
import threading
from typing import Dict

import grpc
import pandas as pd
from psutil import Process

from algorithms.factory import get_agent
from algorithms.agent_interface import SpiceAIAgent
from cleanup import cleanup_on_shutdown
from connector.manager import ConnectorManager, ConnectorName
from connector.stateful import StatefulConnector
from data import DataManager
from exception import InvalidDataShapeException
from proto.aiengine.v1 import aiengine_pb2, aiengine_pb2_grpc
from train import train_agent, training_lock, saved_models
from validation import validate_rewards

data_managers: "dict[DataManager]" = {}
connector_managers: "dict[ConnectorManager]" = {}

training_thread = None
init_lock = threading.Lock()


def dispatch_train_agent(
    pod_name: str,
    data_manager: DataManager,
    connector_manager: ConnectorManager,
    algorithm: str,
    number_episodes: int,
    flight: str,
    training_goal: str,
):
    if training_lock.locked():
        return False

    global training_thread
    training_thread = threading.Thread(
        target=train_agent,
        args=(
            pod_name,
            data_manager,
            connector_manager,
            algorithm,
            number_episodes,
            flight,
            training_goal,
        ),
    )
    training_thread.start()
    return True


class AIEngine(aiengine_pb2_grpc.AIEngineServicer):
    def GetHealth(self, request, context):
        return aiengine_pb2.Response(result="ok")

    def AddData(self, request: aiengine_pb2.AddDataRequest, context):
        with init_lock:
            new_data: pd.DataFrame = pd.read_csv(StringIO(request.csv_data))
            new_data["time"] = pd.to_datetime(new_data["time"], unit="s")
            new_data = new_data.set_index("time")

            data_manager: DataManager = data_managers[request.pod]
            for field in new_data.columns:
                if field not in data_manager.fields.keys():
                    return aiengine_pb2.Response(
                        result="unexpected_field",
                        message=f"Unexpected field: '{field}'",
                        error=True,
                    )

            data_manager.merge_data(new_data)
            return aiengine_pb2.Response(result="ok")

    def AddInterpretations(
        self, request: aiengine_pb2.AddInterpretationsRequest, context
    ):
        data_manager: DataManager = data_managers[request.pod]
        data_manager.add_interpretations(request.indexed_interpretations)
        return aiengine_pb2.Response(result="ok")

    def StartTraining(self, request: aiengine_pb2.StartTrainingRequest, context):
        data_manager: DataManager = data_managers[request.pod]
        connector_manager: ConnectorManager = connector_managers[request.pod]

        if request.epoch_time != 0:
            new_epoch_time = pd.to_datetime(request.epoch_time, unit="s")
            if new_epoch_time < data_manager.epoch_time:
                return aiengine_pb2.Response(
                    result="epoch_time_invalid",
                    message=f"epoch time should be after {data_manager.epoch_time.timestamp()}",
                    error=True,
                )
            data_manager.epoch_time = new_epoch_time
            data_manager.end_time = data_manager.epoch_time + data_manager.period_secs

        algorithm = request.learning_algorithm
        number_episodes = (
            request.number_episodes if request.number_episodes != 0 else 30
        )
        flight = request.flight
        training_goal = request.training_goal

        index_of_epoch = data_manager.massive_table_filled.index.get_loc(
            data_manager.epoch_time, "ffill"
        )

        if (
            len(data_manager.massive_table_filled.iloc[index_of_epoch:])
            < data_manager.get_window_span()
        ):
            return aiengine_pb2.Response(
                result="not_enough_data_for_training",
                error=True,
            )

        started = dispatch_train_agent(
            request.pod,
            data_manager,
            connector_manager,
            algorithm,
            number_episodes,
            flight,
            training_goal,
        )
        result = "started_training" if started else "already_training"
        return aiengine_pb2.Response(result=result)

    def GetInference(self, request: aiengine_pb2.InferenceRequest, context):
        try:
            model_exists = False
            if request.pod in saved_models:
                model_exists = True

            if request.pod not in data_managers:
                return aiengine_pb2.InferenceResult(
                    response=aiengine_pb2.Response(
                        result="pod_not_initialized", error=True
                    )
                )

            if request.tag != "latest":
                return aiengine_pb2.InferenceResult(
                    response=aiengine_pb2.Response(
                        result="tag_not_yet_supported",
                        message="Support for multiple tags coming soon!",
                        error=True,
                    )
                )

            # Ideally we could just re-use the in-memory agent we created during training,
            # but tensorflow has issues with multi-threading in python, so we are just loading it from the file system
            data_manager = data_managers[request.pod]
            model_data_shape = data_manager.get_shape()
            if model_exists:
                save_path = saved_models[request.pod]
                with open(save_path / "meta.json", "r", encoding="utf-8") as meta_file:
                    save_info = json.loads(meta_file.read())
                algorithm = save_info["algorithm"]
            else:
                algorithm = "dql"

            agent: SpiceAIAgent = get_agent(
                algorithm, model_data_shape, len(data_manager.action_names)
            )
            if model_exists:
                agent.load(Path(save_path))

            data_manager: DataManager = data_managers[request.pod]

            if (
                data_manager.massive_table_filled.shape[0]
                < data_manager.get_window_span()
            ):
                return aiengine_pb2.InferenceResult(
                    response=aiengine_pb2.Response(result="not_enough_data", error=True)
                )

            latest_window = data_manager.get_latest_window()
            state = data_manager.flatten_and_normalize_window(latest_window)

            action_from_model, probabilities = agent.act(state)
        except InvalidDataShapeException as ex:
            result = ex.type
            message = ex.message

            if training_lock.locked():
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

        end_time = data_manager.massive_table_filled.last_valid_index()
        start_time = end_time - data_manager.interval_secs

        result = aiengine_pb2.InferenceResult()
        result.start = int(start_time.timestamp())
        result.end = int(end_time.timestamp())
        result.action = action_name
        result.confidence = float(confidence)
        result.tag = request.tag
        result.response.result = "ok"

        return result

    def Init(self, request: aiengine_pb2.InitRequest, context):
        with init_lock:
            data_manager = DataManager()
            connector_manager = ConnectorManager()

            period_secs = pd.to_timedelta(request.period, unit="s")
            interval_secs = pd.to_timedelta(request.interval, unit="s")
            granularity_secs = pd.to_timedelta(request.granularity, unit="s")

            epoch_time = pd.Timestamp.now() - period_secs
            if request.epoch_time != 0:
                epoch_time = pd.to_datetime(request.epoch_time, unit="s")

            if len(request.actions) == 0:
                return aiengine_pb2.Response(result="missing_actions", error=True)
            action_rewards = request.actions
            if not validate_rewards(action_rewards):
                return aiengine_pb2.Response(
                    result="invalid_reward_function", error=True
                )

            if len(request.fields) == 0:
                return aiengine_pb2.Response(result="missing_fields", error=True)

            data_manager.init(
                epoch_time=epoch_time,
                period_secs=period_secs,
                interval_secs=interval_secs,
                granularity_secs=granularity_secs,
                fields=request.fields,
                action_rewards=action_rewards,
                actions_order=request.actions_order,
                laws=request.laws,
            )
            data_managers[request.pod] = data_manager
            connector_managers[request.pod] = connector_manager

            datasources_data = request.datasources

            for datasource_data in datasources_data:
                connector_data = datasource_data.connector
                connector_name: ConnectorName = connector_data.name

                datasource_actions = datasource_data.actions

                if connector_name == ConnectorName.STATEFUL.value:
                    new_connector = StatefulConnector(
                        data_manager=data_manager,
                        action_effects=datasource_actions,
                    )
                    connector_manager.add_connector(new_connector)
            return aiengine_pb2.Response(result="ok")

    def ExportModel(self, request: aiengine_pb2.ExportModelRequest, context):
        if request.pod not in saved_models:
            return aiengine_pb2.ExportModelResult(
                response=aiengine_pb2.Response(
                    result="pod_not_trained",
                    message="Unable to export a model that hasn't finished at least one training run",
                    error=True,
                )
            )

        if request.pod not in data_managers:
            return aiengine_pb2.ExportModelResult(
                resopnse=aiengine_pb2.Response(result="pod_not_initialized", error=True)
            )

        if request.tag != "latest":
            return aiengine_pb2.ExportModelResult(
                response=aiengine_pb2.Response(
                    result="tag_not_yet_supported",
                    message="Support for multiple tags coming soon!",
                    error=True,
                )
            )

        return aiengine_pb2.ExportModelResult(
            response=aiengine_pb2.Response(result="ok"),
            model_path=str(saved_models[request.pod]),
        )

    def ImportModel(self, request: aiengine_pb2.ImportModelRequest, context):
        if request.pod not in data_managers:
            return aiengine_pb2.Response(result="pod_not_initialized", error=True)

        data_manager = data_managers[request.pod]
        model_data_shape = data_manager.get_shape()
        import_path = Path(request.import_path)

        if not (import_path / "meta.json").exists():
            return aiengine_pb2.Response(
                result="unable_to_load_model_metadata",
                message=f"Unable to find meta data at {import_path}",
                error=True,
            )
        with open(import_path / "meta.json", "r", encoding="utf-8") as meta_file:
            algorithm = json.loads(meta_file.read())["algorithm"]

        agent: SpiceAIAgent = get_agent(
            algorithm, model_data_shape, len(data_manager.action_names)
        )
        if not agent.load(import_path):
            return aiengine_pb2.Response(
                result="unable_to_load_model",
                message=f"Unable to find a model at {import_path}",
                error=True,
            )

        saved_models[request.pod] = Path(request.import_path)

        return aiengine_pb2.Response(result="ok")


def wait_parent_process():
    current_process = Process(os.getpid())
    parent_process: Process = current_process.parent()

    parent_process.wait()


def main():
    # Preventing tensorflow verbose initialization
    os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
    import tensorflow as tf  # pylint: disable=import-outside-toplevel

    # Eager execution is too slow to use, so disabling
    tf.compat.v1.disable_eager_execution()

    signal.signal(signal.SIGINT, cleanup_on_shutdown)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    aiengine_pb2_grpc.add_AIEngineServicer_to_server(AIEngine(), server)
    server.add_insecure_port("[::]:8004")
    server.start()
    print(f"AIEngine: gRPC server listening on port {8004}")

    wait_parent_process()
    cleanup_on_shutdown()


if __name__ == "__main__":
    main()
