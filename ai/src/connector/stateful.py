from data import DataManager
import numpy as np
import pandas as pd
import copy
from exception import LawInvalidException, DataSourceActionInvalidException
from exec import somewhat_safe_exec, somewhat_safe_eval


class StatefulConnector:
    def __init__(
        self,
        data_manager: DataManager,
        action_effects: "dict[str]",
    ):
        self.action_effects = action_effects
        self.data_manager = data_manager

    def update_state(self, next_timestamp: pd.Timestamp, new_data: dict):
        new_series = dict()
        for data in new_data:
            new_series[data] = [new_data[data]]
        new_data_frame = pd.DataFrame(new_series, index={next_timestamp})

        self.data_manager.merge_data(new_data_frame)

    def apply_action(self, action: int, data_row: pd.DataFrame) -> bool:
        action_name = self.data_manager.action_names[action]

        if not action_name in self.action_effects:
            return True

        locals = dict()
        for key, value in data_row.items():
            locals[key] = value[-1]

        original_local = copy.deepcopy(locals)

        try:
            locals = somewhat_safe_exec(self.action_effects[action_name], locals)
        except Exception as ex:
            raise DataSourceActionInvalidException(repr(ex))

        try:
            for law in self.data_manager.laws:
                if not somewhat_safe_eval(law, locals):
                    return False
        except Exception as ex:
            raise LawInvalidException(repr(ex))

        del_fields = []
        for field in locals:
            if original_local[field] == locals[field]:
                del_fields.append(field)

        for field in del_fields:
            del locals[field]

        if len(locals) == 0:
            return True

        next_timestamp = data_row.index[-1] + self.data_manager.granularity_secs
        self.update_state(next_timestamp, locals)

        return True
