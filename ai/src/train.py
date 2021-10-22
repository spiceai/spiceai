import math
from pathlib import Path
import tempfile
import threading
import time
from typing import Dict

import numpy as np
import requests

from algorithms.factory import get_agent
from algorithms.agent_interface import SpiceAIAgent
from cleanup import directories_to_delete
from connector.manager import ConnectorManager
from data import DataManager
from exec import somewhat_safe_eval
from exception import DataSourceActionInvalidException, LawInvalidException, RewardInvalidException
from metrics import metrics
from progress import ProgressBar
from utils import print_event


class Trainer():
    TRAINING_LOCK = threading.Lock()
    SAVED_MODELS: Dict[str, Path] = {}

    def __init__(
            self, pod_name: str, data_manager: DataManager, connector_manager: ConnectorManager, algorithm: str,
            number_episodes: int, flight: str, training_goal: str):
        self.pod_name = pod_name
        self.data_manager = data_manager
        self.connector_manager = connector_manager
        self.algorithm = algorithm
        self.number_episodes = number_episodes
        self.flight = flight
        self.training_goal = training_goal

        self.action_size = len(data_manager.action_names)

        self.request_url = f"http://localhost:8000/api/v0.1/pods/{pod_name}/training_runs/{flight}/episodes"

        self.training_episodes = number_episodes
        self.not_learning_threshold = 3

        self.data_manager.rewind()
        self.model_data_shape = data_manager.get_shape()
        self.agent: SpiceAIAgent = get_agent(algorithm, self.model_data_shape, self.action_size)

        self.custom_training_goal_met = False
        self.not_learning_episodes_threshold_met = False

        self.should_stop = False

    def run_episode(self, model_state, raw_state, raw_state_prime_interpretations, progress_bar):
        episode_reward = 0
        episode_actions = [0] * len(self.data_manager.action_names)
        while True:
            metrics.start("episode")
            action, _ = self.agent.act(model_state)
            progress_bar.next()

            try:
                is_valid = self.connector_manager.apply_action(action, raw_state)
            except (DataSourceActionInvalidException, LawInvalidException) as ex:
                post_episode_result(self.request_url, ex.get_error_body())
                self.should_stop = True
                return episode_reward, episode_actions

            if not self.data_manager.advance():
                return episode_reward, episode_actions

            raw_state_prime = self.data_manager.get_current_window()
            model_state_prime = self.data_manager.flatten_and_normalize_window(
                raw_state_prime
            )
            if np.shape(model_state_prime) != self.model_data_shape:
                return episode_reward, episode_actions

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
                    return episode_reward, episode_actions

            episode_reward += reward
            self.agent.add_experience(model_state, action, reward, model_state_prime)
            episode_actions[action] += 1
            model_state = model_state_prime
            raw_state = raw_state_prime
            metrics.end("episode")

    def train(self):
        with self.TRAINING_LOCK:
            print_event(self.pod_name, f"Training {self.training_episodes} episodes...")

            not_learning_episodes = 0
            last_episode_reward = None
            for episode in range(1, self.training_episodes + 1):
                episode_start = math.floor(time.time())
                self.data_manager.rewind()
                raw_state = self.data_manager.get_current_window()
                raw_state_prime_interpretations = self.data_manager.get_interpretations_for_interval()
                model_state = self.data_manager.flatten_and_normalize_window(raw_state)

                total_steps = math.floor(self.data_manager.param.period_secs / self.data_manager.param.granularity_secs)
                progress_bar = ProgressBar(self.pod_name, episode, total_steps)
                metrics.reset()

                episode_reward, episode_actions = self.run_episode(
                    model_state, raw_state, raw_state_prime_interpretations, progress_bar)
                if self.should_stop:
                    return

                episode_end = math.floor(time.time())

                if self.training_goal != "":
                    loc = {}
                    loc["score"] = episode_reward
                    self.custom_training_goal_met = somewhat_safe_eval(self.training_goal, loc)

                self.agent.learn()
                print_event(
                    self.pod_name,
                    f"Episode {episode} completed with score of {round(episode_reward, 2)}.",
                )

                episode_actions_name = {}
                action_counts = ""
                for i in range(self.action_size):
                    action_name = self.data_manager.action_names[i]
                    action_count = episode_actions[i]
                    action_counts += f"{action_name} = {action_count}"
                    episode_actions_name[action_name] = action_count
                    action_counts += (", " if i != (self.action_size - 1) else ".")

                print_event(self.pod_name, f"Action Counts: {action_counts}")

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
                print_event(self.pod_name, f"Training goal '{self.training_goal}' reached!")
            elif self.not_learning_episodes_threshold_met:
                print_event(self.pod_name, "Training goal 'score_variance < 1' reached!")
            else:
                print_event(self.pod_name, f"Max training episodes ({self.training_episodes}) reached!")

        tmpdir = tempfile.mkdtemp(prefix="spiceai_")
        save_path = Path(tmpdir, f"{self.pod_name}_train")
        save_path.mkdir(parents=True)
        directories_to_delete.append(tmpdir)
        self.agent.save(save_path)
        self.SAVED_MODELS[self.pod_name] = save_path


def end_of_episode(_episode: int):
    return True


def post_episode_result(request_url, episode_data):
    try:
        requests.post(request_url, json=episode_data)
    except Exception as error:
        print(f"Failed to update episode result: {error}")
