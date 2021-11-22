from abc import ABC, abstractmethod
import threading
from types import SimpleNamespace
from typing import Dict, List

import pandas as pd
import numpy as np

from exception import RewardInvalidException
from exec import load_module_from_code, somewhat_safe_exec
from metrics import Metrics
from proto.common.v1 import common_pb2
from proto.aiengine.v1 import aiengine_pb2


class DataParam:
    def __init__(self, epoch_time: pd.Timestamp, period_secs: pd.Timedelta, interval_secs: pd.Timedelta,
                 granularity_secs: pd.Timedelta):
        self.interval_secs = interval_secs
        self.granularity_secs = granularity_secs
        self.epoch_time = epoch_time
        self.period_secs = period_secs
        self.end_time = epoch_time + self.period_secs


class DataManagerBase(ABC):
    def __init__(self, param: DataParam, fields: Dict[str, aiengine_pb2.FieldData], action_rewards: Dict[str, str],
                 actions_order: Dict[str, int], external_reward_funcs: str, laws: List[str]):
        self.fields = fields
        self.laws = laws
        self.param = param
        self.metrics = Metrics()

        self.data_frame = pd.DataFrame(columns=fields)
        self.interpretations: common_pb2.IndexedInterpretations = None
        self.action_rewards = action_rewards
        self.action_names = [None] * len(actions_order)

        for action in actions_order:
            self.action_names[actions_order[action]] = action

        self.reward_funcs_module = None
        if len(external_reward_funcs) > 0:
            self.reward_funcs_module = load_module_from_code(external_reward_funcs, "reward_funcs")

            self.reward_funcs_module_actions = {}
            for action_name in self.action_names:
                reward_func_name = self.action_rewards[action_name]
                reward_func = getattr(self.reward_funcs_module, reward_func_name)
                self.reward_funcs_module_actions[action_name] = reward_func
        self.interpretations = None
        self.table_lock = threading.Lock()

    @staticmethod
    def flatten_and_normalize_window(current_window) -> np.ndarray:
        result_array = []
        for col in current_window:
            denominator = np.linalg.norm(current_window[col])
            denominator = 1 if denominator == 0 else denominator

            norm_rows = current_window[col] / denominator

            for row in norm_rows:
                result_array.append(row)
        return np.nan_to_num(result_array)

    def __enter__(self):
        """
        Python Docs: https://docs.python.org/3/library/stdtypes.html#contextmanager.__enter__

        Used to indicate to the datamanager that the pod is currently training. This should
        trigger the data manager to make a deep copy of the underlying data to be used for training.
        """
        self.start_training()

    def __exit__(self, _exc_type, _exc_val, _exc_tb):
        """
        Python Docs: https://docs.python.org/3/library/stdtypes.html#contextmanager.__exit__

        Used to indicate to the datamanager that the pod has stopped training and can clean up the
        copy of the data used for training.
        """
        self.end_training()

    @abstractmethod
    def start_training(self):
        raise RuntimeError('Not Implemented')

    @abstractmethod
    def end_training(self):
        raise RuntimeError('Not Implemented')

    @abstractmethod
    def add_interpretations(self, interpretations):
        raise RuntimeError('Not Implemented')

    @abstractmethod
    def get_current_window(self) -> pd.DataFrame:
        raise RuntimeError('Not Implemented')

    @abstractmethod
    def get_window_at(self, time: pd.Timestamp) -> pd.DataFrame:
        raise RuntimeError('Not Implemented')

    @abstractmethod
    def get_shape(self) -> tuple:
        raise RuntimeError('Not Implemented')

    @abstractmethod
    def merge_data(self, new_data):
        raise RuntimeError('Not Implemented')

    @abstractmethod
    def merge_training_row(self, new_row):
        raise RuntimeError('Not Implemented')

    @abstractmethod
    def advance(self) -> bool:
        raise RuntimeError('Not Implemented')

    @abstractmethod
    def reset(self):
        raise RuntimeError('Not Implemented')

    def reward(
            self, prev_state_pd, prev_state_interpretations,
            new_state_pd, new_state_intepretations, action: int) -> float:
        prev_state_dict = {}
        new_state_dict = {}
        action_name = self.action_names[action]

        for key in prev_state_pd:
            prev_state_dict[key] = list(prev_state_pd[key])[-1]
            new_state_dict[key] = list(new_state_pd[key])[-1]

        if self.reward_funcs_module is not None:
            reward_func = self.reward_funcs_module_actions[action_name]
            return reward_func(prev_state_dict, prev_state_interpretations, new_state_dict, new_state_intepretations)

        prev_state_dict["interpretations"] = prev_state_interpretations
        new_state_dict["interpretations"] = new_state_intepretations

        prev_state = SimpleNamespace(**prev_state_dict)
        new_state = SimpleNamespace(**new_state_dict)

        loc = {}
        loc["prev_state"] = prev_state
        loc["new_state"] = new_state
        loc["print"] = print

        reward_func = self.action_rewards[action_name]

        try:
            loc = somewhat_safe_exec(reward_func, loc)
        except Exception as ex:
            raise RewardInvalidException(repr(ex)) from ex

        return loc["reward"]
