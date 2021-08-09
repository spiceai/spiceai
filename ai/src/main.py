import pandas as pd
import numpy as np
from io import StringIO
import time
import math
import os
import tensorflow as tf
import threading
import requests
import tempfile
from agent import Spice_Agent
from flask import Flask, jsonify, make_response, request
from data import DataManager
from connector.manager import ConnectorManager, ConnectorType
from connector.openai_gym import OpenAIGymConnector
from connector.stateful import StatefulConnector
from validation import validate_rewards
from exception import (
    LawInvalidException,
    RewardInvalidException,
    DataSourceActionInvalidException,
)
from utils import print_event
from progress import ProgressBar
from metrics import metrics

GAMMA = 0.9
LEARNING_RATE = 0.01
HIDDEN_NEURONS = 64

app = Flask(__name__)
data_managers: "dict[DataManager]" = dict()
connector_managers: "dict[ConnectorManager]" = dict()
model_data_shapes = dict()
saved_models: "dict[str]" = dict()

training_lock = threading.Lock()
training_thread = None
init_lock = threading.Lock()

# Tensors passed to custom loss functions don't work for some reason.
# Turning off eager execution makes it work
# https://github.com/ChintanTrivedi/rl-bot-football/issues/2
tf.compat.v1.disable_eager_execution()


def train_agent(
    pod_name: str,
    data_manager: DataManager,
    connector_manager: ConnectorManager,
    number_episodes: int,
    flight: str,
    training_goal: str,
):
    with training_lock:
        ACTION_SIZE = len(data_manager.action_names)

        REQUEST_URL = (
            f"http://localhost:8000/api/v0.1/pods/{pod_name}/flights/{flight}/episodes"
        )

        TRAINING_EPISODES = number_episodes
        NOT_LEARNING_THRESHOLD = 3

        data_manager.rewind()
        model_data_shape = data_manager.get_shape()
        agent = Spice_Agent(
            model_data_shape,
            ACTION_SIZE,
            GAMMA,
            LEARNING_RATE,
            HIDDEN_NEURONS,
        )
        model_data_shapes[pod_name] = model_data_shape

        print_event(pod_name, f"Training {TRAINING_EPISODES} episodes...")

        custom_training_goal_met = False
        not_learning_episodes_threshold_met = False

        not_learning_episodes = 0
        last_episode_reward = None
        for episode in range(TRAINING_EPISODES):
            episode_start = math.floor(time.time())
            data_manager.rewind()
            raw_state = data_manager.get_current_window()
            model_state = data_manager.flatten_and_normalize_window(raw_state)
            episode_reward = 0
            episode_actions = [0] * len(data_manager.action_names)

            total_steps = math.floor(
                data_manager.period_secs / data_manager.granularity_secs
            )
            progress_bar = ProgressBar(pod_name, episode, total_steps)
            metrics.reset()

            while True:
                metrics.start("episode")
                action, _ = agent.act(model_state)
                progress_bar.next()

                try:
                    is_valid = connector_manager.apply_action(action, raw_state)
                except (DataSourceActionInvalidException, LawInvalidException) as ex:
                    post_episode_result(REQUEST_URL, ex.get_error_body())
                    return

                if not data_manager.advance():
                    break

                raw_state_prime = data_manager.get_current_window()
                model_state_prime = data_manager.flatten_and_normalize_window(
                    raw_state_prime
                )
                if np.shape(model_state_prime) != model_data_shape:
                    break

                reward = -5
                if is_valid:
                    try:
                        reward = data_manager.reward(
                            raw_state,
                            raw_state_prime,
                            action,
                        )
                    except RewardInvalidException as ex:
                        post_episode_result(REQUEST_URL, ex.get_error_body())
                        return

                episode_reward += reward
                agent.add_experience((model_state, action, reward))
                episode_actions[action] += 1
                model_state = model_state_prime
                raw_state = raw_state_prime
                metrics.end("episode")

            episode_end = math.floor(time.time())

            if training_goal != "":
                loc = dict()
                loc["score"] = episode_reward
                custom_training_goal_met = eval(training_goal, dict(), loc)

            agent.learn()
            print_event(
                pod_name,
                f"Episode {episode} completed with score of {round(episode_reward, 2)}.",
            )

            episode_actions_name = dict()
            action_counts = ""
            for i in range(ACTION_SIZE):
                action_name = data_manager.action_names[i]
                action_count = episode_actions[i]
                action_counts += f"{action_name} = {action_count}"
                episode_actions_name[action_name] = action_count
                if i != (ACTION_SIZE - 1):
                    action_counts += ", "
                else:
                    action_counts += "."

            print_event(pod_name, f"Action Counts: {action_counts}")

            episode_data = {
                "episode": episode,
                "start": episode_start,
                "end": episode_end,
                "score": round(episode_reward, 2),
                "actions_taken": episode_actions_name,
            }

            post_episode_result(REQUEST_URL, episode_data)

            if last_episode_reward == episode_reward:
                not_learning_episodes += 1
            else:
                not_learning_episodes = 0

            if not_learning_episodes >= NOT_LEARNING_THRESHOLD:
                not_learning_episodes_threshold_met = True
                break

            last_episode_reward = episode_reward

            end_of_episode(episode)

        if custom_training_goal_met:
            print_event(pod_name, f"Training goal '{training_goal}' reached!")
        elif not_learning_episodes_threshold_met:
            print_event(pod_name, f"Training goal 'score_variance < 1' reached!")
        else:
            print_event(
                pod_name, f"Max training episodes ({TRAINING_EPISODES}) reached!"
            )

        tmpdir = os.getenv("TMPDIR") or tempfile.gettempdir()
        model_path = os.path.join(tmpdir, f"{pod_name}.model")
        agent.save(model_path)
        saved_models[pod_name] = model_path


def end_of_episode(episode: int):
    return True


def post_episode_result(request_url, episode_data):
    try:
        requests.post(
            request_url,
            json=episode_data,
        )
    except Exception as e:
        print(f"Failed to update episode result: {e}")
        pass


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
        new_data = pd.read_csv(StringIO(csv_data))
        new_data["time"] = pd.to_datetime(new_data["time"], unit="s")
        new_data = new_data.set_index("time")

        data_manager: DataManager = data_managers[pod]
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
    agent = Spice_Agent(
        model_data_shape,
        len(data_manager.action_names),
        GAMMA,
        LEARNING_RATE,
        HIDDEN_NEURONS,
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
