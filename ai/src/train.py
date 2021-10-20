import math
import os
from pathlib import Path
import tempfile
import threading
import time

import numpy as np
from progress import ProgressBar
import requests

from algorithms.factory import get_agent
from algorithms.agent_interface import SpiceAIAgent
from cleanup import directories_to_delete
from connector.manager import ConnectorManager
from data import DataManager
from exec import somewhat_safe_eval
from exception import (
    DataSourceActionInvalidException,
    LawInvalidException,
    RewardInvalidException,
)
from metrics import metrics
from utils import print_event


training_lock = threading.Lock()

saved_models: "dict[str]" = {}


def train_agent(
    pod_name: str,
    data_manager: DataManager,
    connector_manager: ConnectorManager,
    algorithm: str,
    number_episodes: int,
    flight: str,
    training_goal: str,
):
    with training_lock:
        ACTION_SIZE = len(data_manager.action_names)

        REQUEST_URL = f"http://localhost:8000/api/v0.1/pods/{pod_name}/training_runs/{flight}/episodes"

        TRAINING_EPISODES = number_episodes
        NOT_LEARNING_THRESHOLD = 3

        data_manager.rewind()
        model_data_shape = data_manager.get_shape()
        agent: SpiceAIAgent = get_agent(algorithm, model_data_shape, ACTION_SIZE)

        print_event(pod_name, f"Training {TRAINING_EPISODES} episodes...")

        custom_training_goal_met = False
        not_learning_episodes_threshold_met = False

        not_learning_episodes = 0
        last_episode_reward = None
        for episode in range(1, TRAINING_EPISODES + 1):
            episode_start = math.floor(time.time())
            data_manager.rewind()
            raw_state = data_manager.get_current_window()
            raw_state_prime_interpretations = (
                data_manager.get_interpretations_for_interval()
            )
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

                raw_state_interpretations = raw_state_prime_interpretations
                raw_state_prime_interpretations = (
                    data_manager.get_interpretations_for_interval()
                )

                reward = -5
                if is_valid:
                    try:
                        reward = data_manager.reward(
                            raw_state,
                            raw_state_interpretations,
                            raw_state_prime,
                            raw_state_prime_interpretations,
                            action,
                        )
                    except RewardInvalidException as ex:
                        post_episode_result(REQUEST_URL, ex.get_error_body())
                        return

                episode_reward += reward
                agent.add_experience(model_state, action, reward, model_state_prime)
                episode_actions[action] += 1
                model_state = model_state_prime
                raw_state = raw_state_prime
                metrics.end("episode")

            episode_end = math.floor(time.time())

            if training_goal != "":
                loc = {}
                loc["score"] = episode_reward
                custom_training_goal_met = somewhat_safe_eval(training_goal, loc)

            agent.learn()
            print_event(
                pod_name,
                f"Episode {episode} completed with score of {round(episode_reward, 2)}.",
            )

            episode_actions_name = {}
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
            print_event(pod_name, "Training goal 'score_variance < 1' reached!")
        else:
            print_event(
                pod_name, f"Max training episodes ({TRAINING_EPISODES}) reached!"
            )

        tmpdir = tempfile.mkdtemp(prefix="spiceai_")
        save_path = Path(tmpdir, f"{pod_name}_train")
        save_path.mkdir(parents=True)
        directories_to_delete.append(tmpdir)
        agent.save(save_path)
        saved_models[pod_name] = save_path


def end_of_episode(_episode: int):
    return True


def post_episode_result(request_url, episode_data):
    try:
        requests.post(
            request_url,
            json=episode_data,
        )
    except Exception as error:
        print(f"Failed to update episode result: {error}")
