import copy
from typing import Dict

import pandas as pd

from data import DataManager
from exception import LawInvalidException, DataSourceActionInvalidException
from exec import somewhat_safe_exec, somewhat_safe_eval


class StatefulConnector:
    def __init__(self, data_manager: DataManager, action_effects: Dict[str, str]):
        self.action_effects = action_effects
        self.data_manager = data_manager

    def update_state(self, next_timestamp: pd.Timestamp, new_data: dict):
        new_series = {}
        for data in new_data:
            new_series[data] = [new_data[data]]
        new_data_frame = pd.DataFrame(new_series, index={next_timestamp})

        self.data_manager.merge_data(new_data_frame)

    def apply_action(self, action: int, data_row: pd.DataFrame) -> bool:
        action_name = self.data_manager.action_names[action]

        if action_name not in self.action_effects:
            return True

        local_data = {}
        for key, value in data_row.items():
            local_data[key] = value[-1]

        original_local = copy.deepcopy(local_data)

        try:
            local_data = somewhat_safe_exec(self.action_effects[action_name], local_data)
        except Exception as ex:
            raise DataSourceActionInvalidException(repr(ex)) from ex

        try:
            for law in self.data_manager.laws:
                if not somewhat_safe_eval(law, local_data):
                    return False
        except Exception as ex:
            raise LawInvalidException(repr(ex)) from ex

        del_fields = []
        for field, value in local_data.items():
            if original_local[field] == value:
                del_fields.append(field)

        for field in del_fields:
            del local_data[field]

        if len(local_data) == 0:
            return True

        next_timestamp = data_row.index[-1] + self.data_manager.param.granularity_secs
        self.update_state(next_timestamp, local_data)

        return True
