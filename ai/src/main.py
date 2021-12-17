from concurrent import futures
from io import StringIO
import json
import os
from pathlib import Path
import signal
import sys
import threading
import traceback
from typing import Dict

import grpc
import pandas as pd
from psutil import Process, TimeoutExpired
import requests

from algorithms.factory import get_agent
from algorithms.agent_interface import SpiceAIAgent
from cleanup import cleanup_on_shutdown
from connector.manager import ConnectorManager, ConnectorName
from connector.stateful import StatefulConnector
from data_manager.base_manager import DataParam, DataManagerBase
from data_manager.event_manager import EventDataManager
from data_manager.time_series_manager import TimeSeriesDataManager
from exception import UnexpectedException
from inference import GetInferenceHandler
from proto.aiengine.v1 import aiengine_pb2, aiengine_pb2_grpc
from train import Trainer
from validation import validate_rewards

data_managers: Dict[str, DataManagerBase] = {}
connector_managers: Dict[str, ConnectorManager] = {}

shutdown_event = threading.Event()


class Dispatch:
    TRAINING_THREAD = None
    INIT_LOCK = threading.Lock()


def train_agent(
    pod_name: str,
    data_manager: DataManagerBase,
    connector_manager: ConnectorManager,
    algorithm: str,
    number_episodes: int,
    flight: str,
    training_goal: str,
    training_data_dir: Path,
    loggers,
):
    try:
        Trainer(
            pod_name,
            data_manager,
            connector_manager,
            algorithm,
            number_episodes,
            flight,
            training_goal,
            training_data_dir,
            loggers,
        ).train()
    except Exception:
        sys.stdout.flush()
        request_url = Trainer.BASE_URL + f"/{pod_name}/training_runs/{flight}/episodes"
        requests.post(
            request_url,
            json=UnexpectedException(traceback.format_exc()).get_error_body(),
        )


def dispatch_train_agent(
    pod_name: str,
    data_manager: DataManagerBase,
    connector_manager: ConnectorManager,
    algorithm: str,
    number_episodes: int,
    flight: str,
    training_goal: str,
    training_data_dir: Path,
    loggers,
):
    if Trainer.TRAINING_LOCK.locked():
        return False

    Dispatch.TRAINING_THREAD = threading.Thread(
        target=train_agent,
        args=(
            pod_name,
            data_manager,
            connector_manager,
            algorithm,
            number_episodes,
            flight,
            training_goal,
            training_data_dir,
            loggers,
        ),
    )
    Dispatch.TRAINING_THREAD.start()
    return True


class AIEngine(aiengine_pb2_grpc.AIEngineServicer):
    def GetHealth(self, request, context):
        return aiengine_pb2.Response(result="ok")

    def AddData(self, request: aiengine_pb2.AddDataRequest, context):
        with Dispatch.INIT_LOCK:
            new_data: pd.DataFrame = pd.read_csv(StringIO(request.csv_data))
            new_data["time"] = pd.to_datetime(new_data["time"], unit="s")
            new_data = new_data.set_index("time")

            data_manager = data_managers[request.pod]
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
        data_manager = data_managers[request.pod]
        data_manager.add_interpretations(request.indexed_interpretations)
        return aiengine_pb2.Response(result="ok")

    def StartTraining(self, request: aiengine_pb2.StartTrainingRequest, context):
        data_manager = data_managers[request.pod]
        connector_manager: ConnectorManager = connector_managers[request.pod]

        if request.epoch_time != 0:
            new_epoch_time = pd.to_datetime(request.epoch_time, unit="s")
            if new_epoch_time < data_manager.param.epoch_time:
                return aiengine_pb2.Response(
                    result="epoch_time_invalid",
                    message=f"epoch time should be after {data_manager.param.epoch_time.timestamp()}",
                    error=True,
                )
            data_manager.param.epoch_time = new_epoch_time
            data_manager.param.end_time = (
                data_manager.param.epoch_time + data_manager.param.period_secs
            )

        algorithm = request.learning_algorithm
        number_episodes = (
            request.number_episodes if request.number_episodes != 0 else 30
        )
        flight = request.flight
        training_goal = request.training_goal
        training_data_dir = request.training_data_dir
        training_loggers = request.training_loggers

        if isinstance(data_manager, TimeSeriesDataManager):
            index_of_epoch = data_manager.massive_table_sparse.index.get_loc(
                data_manager.param.epoch_time, "ffill"
            )
            if (
                len(data_manager.massive_table_sparse.iloc[index_of_epoch:])
                < data_manager.get_window_span()
            ):
                return aiengine_pb2.Response(
                    result="not_enough_data_for_training", error=True
                )

        started = dispatch_train_agent(
            request.pod,
            data_manager,
            connector_manager,
            algorithm,
            number_episodes,
            flight,
            training_goal,
            training_data_dir,
            training_loggers,
        )
        result = "started_training" if started else "already_training"
        return aiengine_pb2.Response(result=result)

    def GetInference(self, request: aiengine_pb2.InferenceRequest, context):
        handler = GetInferenceHandler(request, data_managers)
        return handler.get_result()

    def Init(self, request: aiengine_pb2.InitRequest, context):
        with Dispatch.INIT_LOCK:
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
            if not validate_rewards(action_rewards, request.external_reward_funcs):
                return aiengine_pb2.Response(
                    result="invalid_reward_function", error=True
                )

            if len(request.fields) == 0:
                return aiengine_pb2.Response(result="missing_fields", error=True)

            data_manager: DataManagerBase = None
            if request.interpolation:
                data_manager = TimeSeriesDataManager(
                    param=DataParam(
                        epoch_time=epoch_time,
                        period_secs=period_secs,
                        interval_secs=interval_secs,
                        granularity_secs=granularity_secs,
                    ),
                    fields=request.fields,
                    action_rewards=action_rewards,
                    actions_order=request.actions_order,
                    external_reward_funcs=request.external_reward_funcs,
                    laws=request.laws,
                )
            else:
                data_manager = EventDataManager(
                    param=DataParam(
                        epoch_time=epoch_time,
                        period_secs=period_secs,
                        interval_secs=interval_secs,
                        granularity_secs=granularity_secs,
                    ),
                    fields=request.fields,
                    action_rewards=action_rewards,
                    actions_order=request.actions_order,
                    external_reward_funcs=request.external_reward_funcs,
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
        if request.pod not in Trainer.SAVED_MODELS:
            return aiengine_pb2.ExportModelResult(
                response=aiengine_pb2.Response(
                    result="pod_not_trained",
                    message="Unable to export a model that hasn't finished at least one training run",
                    error=True,
                )
            )

        if request.pod not in data_managers:
            return aiengine_pb2.ExportModelResult(
                response=aiengine_pb2.Response(result="pod_not_initialized", error=True)
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
            model_path=str(Trainer.SAVED_MODELS[request.pod]),
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
            algorithm, model_data_shape, len(data_manager.action_names), None, None
        )
        if not agent.load(import_path):
            return aiengine_pb2.Response(
                result="unable_to_load_model",
                message=f"Unable to find a model at {import_path}",
                error=True,
            )

        Trainer.SAVED_MODELS[request.pod] = Path(request.import_path)

        return aiengine_pb2.Response(result="ok")


def wait_parent_process():
    current_process = Process(os.getpid())
    parent_process: Process = current_process.parent()

    while True:
        try:
            parent_process.wait(0.1)
        except TimeoutExpired:
            if shutdown_event.is_set():
                return
            continue


def interrupt_handler(_signum, _frame):
    shutdown_event.set()
    print("\r  ")


def main():
    # Preventing tensorflow verbose initialization
    os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
    import tensorflow as tf  # pylint: disable=import-outside-toplevel

    # Eager execution is too slow to use, so disabling
    tf.compat.v1.disable_eager_execution()

    signal.signal(signal.SIGINT, interrupt_handler)
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    aiengine_pb2_grpc.add_AIEngineServicer_to_server(AIEngine(), server)
    server.add_insecure_port("[::]:8004")
    server.start()
    print(f"AIEngine: gRPC server listening on port {8004}")

    wait_parent_process()
    cleanup_on_shutdown()
    grpc_shutdown = server.stop(grace=0.1)
    grpc_shutdown.wait()


if __name__ == "__main__":
    main()
