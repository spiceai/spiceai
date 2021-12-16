import math
from pathlib import Path
import tempfile
import threading
import time
from typing import Dict

import requests

from algorithms.factory import get_agent
from algorithms.agent_interface import SpiceAIAgent
from cleanup import directories_to_delete
from connector.manager import ConnectorManager
from data_manager.base_manager import DataManagerBase
from data_manager.time_series_manager import TimeSeriesDataManager
from exec import somewhat_safe_eval
from exception import (
    DataSourceActionInvalidException,
    LawInvalidException,
    RewardInvalidException,
)
from progress import ProgressBar
from utils import print_event


class Trainer:
    TRAINING_LOCK = threading.Lock()
    SAVED_MODELS: Dict[str, Path] = {}
    BASE_URL = "http://localhost:8000/api/v0.1/pods"

    def __init__(
        self,
        pod_name: str,
        data_manager: DataManagerBase,
        connector_manager: ConnectorManager,
        algorithm: str,
        number_episodes: int,
        flight: str,
        training_goal: str,
        training_data_dir: str,
        training_loggers,
    ):
        self.pod_name = pod_name
        self.data_manager = data_manager
        self.connector_manager = connector_manager
        self.algorithm = algorithm
        self.number_episodes = number_episodes
        self.flight = flight
        self.training_goal = training_goal
        self.training_data_dir = training_data_dir
        self.training_loggers = training_loggers

        self.action_size = len(data_manager.action_names)

        self.request_url = (
            self.BASE_URL + f"/{pod_name}/training_runs/{flight}/episodes"
        )

        self.training_episodes = number_episodes
        self.not_learning_threshold = 3

        self.log_dir = Path(self.training_data_dir, "log")

        self.model_data_shape = data_manager.get_shape()
        self.agent: SpiceAIAgent = get_agent(
            algorithm,
            self.model_data_shape,
            self.action_size,
            self.training_loggers,
            self.log_dir,
        )

        self.custom_training_goal_met = False
        self.not_learning_episodes_threshold_met = False

        self.should_stop = False

    def run_episode(
        self, model_state, raw_state, raw_state_prime_interpretations, progress_bar
    ):
        episode_reward = 0
        episode_actions = [0] * len(self.data_manager.action_names)
        while True:
            self.data_manager.metrics.start("episode")
            action, _ = self.agent.act(model_state)
            progress_bar.next()

            try:
                is_valid = self.connector_manager.apply_action(action, raw_state)
            except (DataSourceActionInvalidException, LawInvalidException) as ex:
                post_episode_result(self.request_url, ex.get_error_body())
                self.should_stop = True
                break

            if not self.data_manager.advance():
                break

            raw_state_prime = self.data_manager.get_current_window()
            model_state_prime = self.data_manager.flatten_and_normalize_window(
                raw_state_prime
            )
            if model_state_prime.shape != self.model_data_shape:
                break

            raw_state_interpretations = raw_state_prime_interpretations
            raw_state_prime_interpretations = (
                self.data_manager.get_interpretations_for_interval()
            )

            reward = -5
            if is_valid:
                try:
                    reward = self.data_manager.reward(
                        raw_state,
                        raw_state_interpretations,
                        raw_state_prime,
                        raw_state_prime_interpretations,
                        action,
                    )
                except RewardInvalidException as ex:
                    post_episode_result(self.request_url, ex.get_error_body())
                    self.should_stop = True
                    break

            episode_reward += reward
            self.agent.add_experience(model_state, action, reward, model_state_prime)
            episode_actions[action] += 1
            model_state = model_state_prime
            raw_state = raw_state_prime
            self.data_manager.metrics.end("episode")

        return episode_reward, episode_actions

    def train(self):
        with self.TRAINING_LOCK, self.data_manager:
            print_event(self.pod_name, f"Training {self.training_episodes} episodes...")

            not_learning_episodes = 0
            last_episode_reward = None
            for episode in range(1, self.training_episodes + 1):
                episode_start = math.floor(time.time())
                self.data_manager.reset()
                raw_state = self.data_manager.get_current_window()
                raw_state_prime_interpretations = (
                    self.data_manager.get_interpretations_for_interval()
                )
                model_state = self.data_manager.flatten_and_normalize_window(raw_state)

                total_steps = (
                    math.floor(
                        self.data_manager.param.period_secs
                        / self.data_manager.param.granularity_secs
                    )
                    if isinstance(self.data_manager, TimeSeriesDataManager)
                    else len(self.data_manager.data_frame)
                )
                progress_bar = ProgressBar(
                    self.pod_name, episode, total_steps, self.data_manager.metrics
                )
                self.data_manager.metrics.reset()

                episode_reward, episode_actions = self.run_episode(
                    model_state,
                    raw_state,
                    raw_state_prime_interpretations,
                    progress_bar,
                )
                if self.should_stop:
                    return

                episode_end = math.floor(time.time())

                if self.training_goal != "":
                    loc = {}
                    loc["score"] = episode_reward
                    self.custom_training_goal_met = somewhat_safe_eval(
                        self.training_goal, loc
                    )

                self.agent.learn()
                print_event(
                    self.pod_name,
                    f"Episode {episode} completed with score of {round(episode_reward, 2)}.",
                )

                episode_actions_name = dict(
                    zip(self.data_manager.action_names, episode_actions)
                )
                print_event(
                    self.pod_name,
                    "Action Counts: "
                    + ", ".join(
                        [
                            f"{action_name} = {action_count}"
                            for action_name, action_count in episode_actions_name.items()
                        ]
                    )
                    + ".",
                )

                episode_data = {
                    "episode": episode,
                    "start": episode_start,
                    "end": episode_end,
                    "score": round(episode_reward, 2),
                    "actions_taken": episode_actions_name,
                }

                post_episode_result(self.request_url, episode_data)
                if last_episode_reward == episode_reward:
                    not_learning_episodes += 1
                else:
                    not_learning_episodes = 0

                if not_learning_episodes >= self.not_learning_threshold:
                    self.not_learning_episodes_threshold_met = True
                    break

                last_episode_reward = episode_reward

                end_of_episode(episode)

            if self.custom_training_goal_met:
                print_event(
                    self.pod_name, f"Training goal '{self.training_goal}' reached!"
                )
            elif self.not_learning_episodes_threshold_met:
                print_event(
                    self.pod_name, "Training goal 'score_variance < 1' reached!"
                )
            else:
                print_event(
                    self.pod_name,
                    f"Max training episodes ({self.training_episodes}) reached!",
                )

        self.agent.save(self.training_data_dir)
        self.SAVED_MODELS[self.pod_name] = self.training_data_dir


def end_of_episode(_episode: int):
    return True


def post_episode_result(request_url, episode_data):
    try:
        requests.post(request_url, json=episode_data)
    except Exception as error:
        print(f"Failed to update episode result: {error}")
