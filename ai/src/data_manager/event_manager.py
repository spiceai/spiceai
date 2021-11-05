from types import SimpleNamespace
from typing import Dict, List

import pandas as pd

from data_manager.base_manager import DataManagerBase, DataParam
from exception import RewardInvalidException
from metrics import metrics
from proto.common.v1 import common_pb2
from proto.aiengine.v1 import aiengine_pb2
from exec import somewhat_safe_exec, load_module_from_code


class EventDataManager(DataManagerBase):
    def __init__(self, param: DataParam, fields: Dict[str, aiengine_pb2.FieldData], action_rewards: Dict[str, str],
                 actions_order: Dict[str, int], external_reward_funcs: str, laws: List[str]):
        self.fields = fields
        self.laws = laws
        self.param = param

        self.data_frame = pd.DataFrame(columns=fields)
        self.interpretations: common_pb2.IndexedInterpretations = None
        self.action_rewards = action_rewards
        self.action_names = [None] * len(actions_order)

        self.current_index = 0

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

    def add_interpretations(self, interpretations):
        self.interpretations = interpretations

    def get_interpretations_for_interval(self):
        if self.interpretations is not None:
            index = self.interpretations.index[int(self.current_time.timestamp())]
            if index is not None and index.indicies is not None and len(index.indicies) > 0:
                interval_interpretations = []
                for i in index.indicies:
                    interval_interpretations.append(
                        self.interpretations.interpretations[i]
                    )
                return interval_interpretations
        return None

    def get_current_window(self) -> pd.DataFrame:
        return self.data_frame[self.current_index: self.current_index + 1]

    def get_shape(self) -> tuple:
        return (self.data_frame.shape[1],)

    def merge_data(self, new_data: pd.DataFrame):
        if len(new_data) == 0:
            return

        if self.data_frame.empty:
            self.data_frame = new_data
        else:
            metrics.start("combine")
            self.data_frame = pd.concat([self.data_frame, new_data], copy=False)
            metrics.end("combine")

    def advance(self) -> bool:
        if self.current_index >= len(self.data_frame):
            return False

        self.current_index += 1
        return True

    def reset(self):
        self.current_index = 0

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
