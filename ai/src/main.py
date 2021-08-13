import pandas as pd
from io import StringIO
import tensorflow as tf
import threading
from algorithms.factory import get_agent
from algorithms.agent_interface import SpiceAIAgent
from flask import Flask, jsonify, make_response, request
from data import DataManager
from connector.manager import ConnectorManager, ConnectorType
from connector.openai_gym import OpenAIGymConnector
from connector.stateful import StatefulConnector
from validation import validate_rewards
from train import train_agent, training_lock, saved_models, ALGORITHM

app = Flask(__name__)
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


@app.route("/pods/<pod>/init", methods=["POST"])
def init(pod: str):
    with init_lock:
        init_data = request.get_json()
        data_manager = DataManager()
        connector_manager = ConnectorManager()

        period_secs = pd.to_timedelta(int(init_data["period"]), unit="s")
        interval_secs = pd.to_timedelta(int(init_data["interval"]), unit="s")
        granularity_secs = pd.to_timedelta(int(init_data["granularity"]), unit="s")

        epoch_time = pd.Timestamp.now() - period_secs
        if "epoch_time" in init_data:
            epoch_time = pd.to_datetime(int(init_data["epoch_time"]), unit="s")

        if not "actions" in init_data:
            return make_response(jsonify({"result": "missing_actions"}), 400)
        action_rewards = init_data["actions"]
        if not validate_rewards(action_rewards):
            return make_response(jsonify({"result": "invalid_reward_function"}), 400)

        if not "fields" in init_data:
            return make_response(jsonify({"result": "missing_fields"}), 400)
        if not "laws" in init_data:
            return make_response(jsonify({"result": "missing_laws"}), 400)

        data_manager.init(
            epoch_time=epoch_time,
            period_secs=period_secs,
            interval_secs=interval_secs,
            granularity_secs=granularity_secs,
            fields=init_data["fields"],
            action_rewards=action_rewards,
            laws=init_data["laws"],
        )
        data_managers[pod] = data_manager
        connector_managers[pod] = connector_manager

        datasources_data = list()
        if "datasources" in init_data:
            datasources_data = init_data["datasources"]

        for datasource_data in datasources_data:
            connector_data = datasource_data["connector"]
            connector_type: ConnectorType = connector_data["type"]

            connector_params = dict()
            if "params" in connector_data:
                connector_params = connector_data["params"]

            datasource_actions = dict()
            if "actions" in datasource_data:
                datasource_actions = datasource_data["actions"]

            if connector_type == ConnectorType.STATEFUL.value:
                new_connector = StatefulConnector(
                    data_manager=data_manager,
                    action_effects=datasource_actions,
                )
                connector_manager.add_connector(new_connector)
            elif connector_type == ConnectorType.OPENAI_GYM.value:
                if not "environment" in connector_params:
                    message = f"missing the environment parameter for {connector_type}"
                    return make_response(
                        jsonify({"result": "missing_param", "message": message}), 400
                    )

                new_connector = OpenAIGymConnector(
                    connector_params["environment"],
                    data_manager,
                    data_manager.fields,
                )
                connector_manager.add_connector(new_connector)
        return make_response(jsonify({"result": "ok"}), 200)


@app.route("/pods/<pod>/data", methods=["POST"])
def data(pod: str):
    with init_lock:
        csv_data = request.get_data().decode("utf-8")
        new_data: pd.DataFrame = pd.read_csv(StringIO(csv_data))
        new_data["time"] = pd.to_datetime(new_data["time"], unit="s")
        new_data = new_data.set_index("time")

        data_manager: DataManager = data_managers[pod]
        for field in new_data.columns:
            if not field in data_manager.fields.keys():
                return make_response(
                    jsonify(
                        {
                            "result": "unexpected_field",
                            "message": f"Unexpected field: '{field}'",
                        }
                    ),
                    400,
                )

        data_manager.merge_data(new_data)
        return make_response(data_manager.massive_table_filled.to_json(), 200)


@app.route("/pods/<pod>/data", methods=["GET"])
def get_data(pod: str):
    data_manager: DataManager = data_managers[pod]
    return make_response(data_manager.massive_table_filled.to_json(), 200)


@app.route("/pods/<pod>/train", methods=["POST"])
def start_training(pod: str):
    train_data = request.get_json()
    data_manager: DataManager = data_managers[pod]
    connector_manager: ConnectorManager = connector_managers[pod]

    if "epoch_time" in train_data:
        new_epoch_time = pd.to_datetime(int(train_data["epoch_time"]), unit="s")
        if new_epoch_time < data_manager.epoch_time:
            invalid_result = {
                "result": "epoch_time_invalid",
                "message": f"epoch time should be after {data_manager.epoch_time.timestamp()}",
            }
            return make_response(jsonify(invalid_result), 400)
        data_manager.epoch_time = new_epoch_time
        data_manager.end_time = data_manager.epoch_time + data_manager.period_secs

    number_episodes = 30
    if "number_episodes" in train_data:
        number_episodes = int(train_data["number_episodes"])

    flight = str(train_data["flight"])

    training_goal = ""
    if "training_goal" in train_data:
        training_goal = str(train_data["training_goal"])

    index_of_epoch = data_manager.massive_table_filled.index.get_loc(
        data_manager.epoch_time, "ffill"
    )

    if (
        len(data_manager.massive_table_filled.iloc[index_of_epoch:])
        < data_manager.get_window_span()
    ):
        return make_response(jsonify({"result": "not_enough_data_for_training"}), 400)

    started = dispatch_train_agent(
        pod, data_manager, connector_manager, number_episodes, flight, training_goal
    )
    message = "started_training" if started else "already_training"
    return make_response(jsonify({"result": message}), 200)


@app.route("/pods/<pod>/models/<tag>/inference", methods=["GET"])
def inference(pod, tag):
    model_exists = False
    if pod in saved_models:
        model_exists = True

    if not pod in data_managers:
        return make_response(
            jsonify({"result": "pod_not_initialized"}),
            404,
        )

    # Ideally we could just re-use the in-memory agent we created during training, but tensorflow has issues with
    # multi-threading in python, so we are just loading it from the file system
    data_manager = data_managers[pod]
    model_data_shape = data_manager.get_shape()
    agent: SpiceAIAgent = get_agent(
        ALGORITHM, model_data_shape, len(data_manager.action_names)
    )
    if model_exists:
        agent.load(saved_models[pod])

    data_manager: DataManager = data_managers[pod]
    latest_window = data_manager.get_latest_window()
    state = data_manager.flatten_and_normalize_window(latest_window)

    action_from_model, probabilities = agent.act(state)
    confidence = f"{probabilities[action_from_model]:.3f}"
    if not model_exists:
        confidence = 0.0

    action_name = data_manager.action_names[action_from_model]

    end_time = data_manager.massive_table_filled.last_valid_index()
    start_time = end_time - data_manager.interval_secs

    response = dict()
    response["start"] = start_time.strftime("%c")
    response["end"] = end_time.strftime("%c")
    response["action"] = action_name
    response["confidence"] = float(confidence)
    response["tag"] = tag

    return make_response(
        jsonify(response),
        200,
    )


@app.route("/health", methods=["GET"])
def health():
    return make_response("ok", 200)


if __name__ == "__main__":

    app.run(host="0.0.0.0", port=8004)
