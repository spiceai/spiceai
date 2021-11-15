from typing import Dict, List

import pandas as pd

from data_manager.base_manager import DataManagerBase, DataParam
from proto.aiengine.v1 import aiengine_pb2


class EventDataManager(DataManagerBase):
    def __init__(self, param: DataParam, fields: Dict[str, aiengine_pb2.FieldData], action_rewards: Dict[str, str],
                 actions_order: Dict[str, int], external_reward_funcs: str, laws: List[str]):
        super().__init__(param, fields, action_rewards, actions_order, external_reward_funcs, laws)

        self.data_frame = pd.DataFrame(columns=fields)
        self.current_index = 0

    def add_interpretations(self, interpretations):
        self.interpretations = interpretations

    def get_interpretations_for_interval(self):
        if self.interpretations is not None:
            index = self.current_index
            if index is not None and index.indicies is not None and len(index.indicies) > 0:
                interval_interpretations = []
                for i in index.indicies:
                    interval_interpretations.append(self.interpretations.interpretations[i])
                return interval_interpretations
        return None

    def get_current_window(self) -> pd.DataFrame:
        with self.table_lock:
            return self.data_frame[self.current_index: self.current_index + 1]

    def get_shape(self) -> tuple:
        return (self.data_frame.shape[1],)

    def merge_data(self, new_data: pd.DataFrame):
        if len(new_data) == 0:
            return

        if self.data_frame.empty:
            self.data_frame = new_data
        else:
            self.metrics.start("combine")
            self.data_frame = pd.concat([self.data_frame, new_data], copy=False)
            self.metrics.end("combine")

    def advance(self) -> bool:
        if self.current_index >= len(self.data_frame):
            return False

        self.current_index += 1
        return True

    def reset(self):
        self.current_index = 0
