import grpc
import pandas as pd
from io import StringIO
import tensorflow as tf
import threading
from psutil import Process
import os
from algorithms.factory import get_agent
from algorithms.agent_interface import SpiceAIAgent
from data import DataManager
from connector.manager import ConnectorManager, ConnectorName
from connector.openai_gym import OpenAIGymConnector
from connector.stateful import StatefulConnector
from validation import validate_rewards
from train import train_agent, training_lock, saved_models, ALGORITHM
from concurrent import futures
from proto.aiengine.v1 import aiengine_pb2, aiengine_pb2_grpc

data_managers: "dict[DataManager]" = dict()
connector_managers: "dict[ConnectorManager]" = dict()

training_thread = None
init_lock = threading.Lock()

# Eager execution is too slow to use, so disabling
tf.compat.v1.disable_eager_execution()


def dispatch_train_agent(
    pod_name: str,
    data_manager: DataManager,
    connector_manager: ConnectorManager,
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
                if not field in data_manager.fields.keys():
                    return aiengine_pb2.Response(
                        result="unexpected_field",
                        message=f"Unexpected field: '{field}'",
                        error=True,
                    )

            data_manager.merge_data(new_data)
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

        number_episodes = 30
        if request.number_episodes != 0:
            number_episodes = request.number_episodes

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
            number_episodes,
            flight,
            training_goal,
        )
        result = "started_training" if started else "already_training"
        return aiengine_pb2.Response(result=result)

    def GetInference(self, request: aiengine_pb2.InferenceRequest, context):
        model_exists = False
        if request.pod in saved_models:
            model_exists = True

        if not request.pod in data_managers:
            return aiengine_pb2.InferenceResult(
                response=aiengine_pb2.Response(result="pod_not_initialized", error=True)
            )

        # Ideally we could just re-use the in-memory agent we created during training, but tensorflow has issues with
        # multi-threading in python, so we are just loading it from the file system
        data_manager = data_managers[request.pod]
        model_data_shape = data_manager.get_shape()
        agent: SpiceAIAgent = get_agent(
            ALGORITHM, model_data_shape, len(data_manager.action_names)
        )
        if model_exists:
            agent.load(saved_models[request.pod])

        data_manager: DataManager = data_managers[request.pod]

        if data_manager.massive_table_filled.shape[0] < data_manager.get_window_span():
            return aiengine_pb2.InferenceResult(
                response=aiengine_pb2.Response(result="not_enough_data", error=True)
            )

        latest_window = data_manager.get_latest_window()
        state = data_manager.flatten_and_normalize_window(latest_window)

        action_from_model, probabilities = agent.act(state)
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
                laws=request.laws,
            )
            data_managers[request.pod] = data_manager
            connector_managers[request.pod] = connector_manager

            datasources_data = request.datasources

            for datasource_data in datasources_data:
                connector_data = datasource_data.connector
                connector_name: ConnectorName = connector_data.name

                connector_params = connector_data.params
                datasource_actions = datasource_data.actions

                if connector_name == ConnectorName.STATEFUL.value:
                    new_connector = StatefulConnector(
                        data_manager=data_manager,
                        action_effects=datasource_actions,
                    )
                    connector_manager.add_connector(new_connector)
                elif connector_name == ConnectorName.OPENAI_GYM.value:
                    if not "environment" in connector_params:
                        message = (
                            f"missing the environment parameter for {connector_name}"
                        )
                        return aiengine_pb2.Response(
                            result="missing_param", message=message, error=True
                        )

                    new_connector = OpenAIGymConnector(
                        connector_params["environment"],
                        data_manager,
                        data_manager.fields,
                    )
                    connector_manager.add_connector(new_connector)
            return aiengine_pb2.Response(result="ok")


def wait_parent_process():
    current_process = Process(os.getpid())
    parent_process: Process = current_process.parent()

    parent_process.wait()


if __name__ == "__main__":
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    aiengine_pb2_grpc.add_AIEngineServicer_to_server(AIEngine(), server)
    server.add_insecure_port("[::]:8004")
    server.start()
    print(f"AIEngine: gRPC server listening on port {8004}")

    wait_parent_process()
