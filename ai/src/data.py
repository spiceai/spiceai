import math
import threading
from types import SimpleNamespace
from typing import Dict, List

import numpy as np
import pandas as pd
from pandas.core.computation import expressions

from exception import RewardInvalidException
from metrics import metrics
from proto.common.v1 import common_pb2
from proto.aiengine.v1 import aiengine_pb2
from exec import somewhat_safe_exec


class DataParam:
    def __init__(self, epoch_time: pd.Timestamp, period_secs: pd.Timedelta, interval_secs: pd.Timedelta,
                 granularity_secs: pd.Timedelta):
        self.interval_secs = interval_secs
        self.granularity_secs = granularity_secs
        self.epoch_time = epoch_time
        self.period_secs = period_secs
        self.end_time = epoch_time + self.period_secs


class DataManager:
    def __init__(self, param: DataParam, fields: Dict[str, aiengine_pb2.FieldData], action_rewards: Dict[str, str],
                 actions_order: Dict[str, int], laws: List[str]):
        self.fields = fields
        self.laws = laws
        self.param = param

        new_series = {}
        for field_name in fields:
            new_series[field_name] = [fields[field_name].initializer]

        self.massive_table_sparse = pd.DataFrame(new_series, index={self.param.epoch_time})
        self.massive_table_sparse = self.massive_table_sparse.resample(self.param.granularity_secs).mean()
        self.massive_table_filled = None
        self.fill_table()

        self.interpretations: common_pb2.IndexedInterpretations = None

        self.current_time: pd.Timestamp = None
        self.action_rewards = action_rewards
        self.table_lock = threading.Lock()

        self.action_names = [None] * len(actions_order)

        for action in actions_order:
            self.action_names[actions_order[action]] = action

    def get_window_span(self):
        return math.floor(self.param.interval_secs / self.param.granularity_secs)

    def overwrite_data(self, new_data):
        self.massive_table_sparse = new_data

        self.fill_table()

    def fill_table(self):
        metrics.start("resample")
        self.massive_table_sparse = self.massive_table_sparse.resample(self.param.granularity_secs).mean()
        metrics.end("resample")

        metrics.start("ffill")
        self.massive_table_filled = self.massive_table_sparse.copy()
        for col_name in self.massive_table_sparse:
            fill_method = self.fields[col_name].fill_method
            if fill_method == aiengine_pb2.FILL_FORWARD:
                self.massive_table_filled[col_name] = self.massive_table_sparse[
                    col_name
                ].ffill()
            elif fill_method == aiengine_pb2.FILL_ZERO:
                self.massive_table_filled[col_name] = self.massive_table_sparse[
                    col_name
                ].fillna(0)
        metrics.end("ffill")

        metrics.start("reindex")
        self.massive_table_filled.index = (
            self.massive_table_filled.index.drop_duplicates(keep="first")
        )
        metrics.end("reindex")

    def merge_row(self, new_row):
        index = new_row.index[0]
        for column_name in list(new_row.keys()):
            value = new_row[column_name].array[0]

            self.massive_table_sparse.loc[index][column_name] = value

        metrics.start("ffill")
        self.massive_table_filled = self.massive_table_sparse.ffill()
        metrics.end("ffill")

    def merge_data(self, new_data):
        def combiner(existing, newer):
            existing_values = (
                existing.values if hasattr(existing, "values") else existing
            )
            newer_values = newer.values if hasattr(newer, "values") else newer

            # use newer values if they aren't nan
            condition = pd.isnull(newer_values)

            return expressions.where(condition, existing_values, newer_values)

        with self.table_lock:
            if len(new_data) == 0:
                return

            if len(new_data) == 1 and new_data.index[0] in self.massive_table_sparse.index:
                metrics.start("merge_row")
                self.merge_row(new_data)
                metrics.end("merge_row")
                return

            metrics.start("combine")
            self.massive_table_sparse = self.massive_table_sparse.combine(
                new_data, combiner
            )
            metrics.end("combine")

            self.fill_table()

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

    def get_shape(self):
        return np.shape([0] * self.get_window_span() * len(self.fields))

    @staticmethod
    def flatten_and_normalize_window(current_window):
        result_array = []
        for col in current_window:
            denominator = np.linalg.norm(current_window[col])
            denominator = 1 if denominator == 0 else denominator

            norm_rows = current_window[col] / denominator

            for row in norm_rows:
                result_array.append(row)
        return np.nan_to_num(result_array)

    def get_current_window(self):
        with self.table_lock:
            # This will get the nearest previous index that matches the timestamp,
            # so we don't need to specify the timestamps exactly
            start_index = self.massive_table_filled.index.get_loc(
                self.current_time - self.param.interval_secs, "ffill"
            )
            end_index = self.massive_table_filled.index.get_loc(
                self.current_time, "ffill"
            )

            return (
                self.massive_table_filled.iloc[start_index:start_index + 1]
                if self.get_window_span() == 1 else
                self.massive_table_filled.iloc[start_index:end_index])

    def get_latest_window(self):
        start_index = None
        end_index = None
        latest_time = self.massive_table_filled.last_valid_index()

        # If we only have a single row, use it
        if self.massive_table_filled.shape[0] == 1:
            start_index = self.massive_table_filled.index.get_loc(latest_time)
            end_index = start_index
        else:
            start_index = self.massive_table_filled.index.get_loc(
                latest_time - self.param.interval_secs, "ffill"
            )
            end_index = self.massive_table_filled.index.get_loc(latest_time, "ffill")

        return (
            self.massive_table_filled.iloc[start_index:start_index + 1]
            if self.get_window_span() == 1 else
            self.massive_table_filled.iloc[start_index:end_index])

    def rewind(self):
        self.current_time = self.param.epoch_time + self.param.interval_secs

    def advance(self):
        if self.current_time >= self.param.end_time:
            return False

        self.current_time += self.param.granularity_secs

        return True

    def reward(
        self,
        prev_state_pd,
        prev_state_interpretations,
        new_state_pd,
        new_state_intepretations,
        action: int,
    ):
        prev_state_dict = {}
        new_state_dict = {}

        for key in prev_state_pd:
            prev_state_dict[key] = list(prev_state_pd[key])[-1]
            new_state_dict[key] = list(new_state_pd[key])[-1]

        prev_state_dict["interpretations"] = prev_state_interpretations
        new_state_dict["interpretations"] = new_state_intepretations

        prev_state = SimpleNamespace(**prev_state_dict)
        new_state = SimpleNamespace(**new_state_dict)

        loc = {}
        loc["prev_state"] = prev_state
        loc["new_state"] = new_state
        loc["print"] = print

        action_name = self.action_names[action]
        reward_func = self.action_rewards[action_name]

        try:
            loc = somewhat_safe_exec(reward_func, loc)
        except Exception as ex:
            raise RewardInvalidException(repr(ex)) from ex

        return loc["reward"]
