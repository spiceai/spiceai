import math
from typing import Dict, List

import numpy as np
import pandas as pd
from pandas.core.computation import expressions

from data_manager.base_manager import DataManagerBase, DataParam
from proto.aiengine.v1 import aiengine_pb2


class TimeSeriesDataManager(DataManagerBase):
    def __init__(self, param: DataParam, fields: Dict[str, aiengine_pb2.FieldData], action_rewards: Dict[str, str],
                 actions_order: Dict[str, int], external_reward_funcs: str, laws: List[str], dataspace_hash: str):
        super().__init__(param, fields, action_rewards, actions_order, external_reward_funcs, laws, dataspace_hash)

        new_series = {}
        for field_name in fields:
            new_series[field_name] = [fields[field_name].initializer]

        self.massive_table_sparse = pd.DataFrame(new_series, index={self.param.epoch_time})
        self.massive_table_sparse = self.massive_table_sparse.resample(self.param.granularity_secs).mean()
        self.massive_table_filled = None
        self._fill_table()

        self.current_time: pd.Timestamp = None

    def get_window_span(self):
        return math.floor(self.param.interval_secs / self.param.granularity_secs)

    def overwrite_data(self, new_data):
        self.massive_table_sparse = new_data

        self._fill_table()

    def _fill_table(self):
        self.metrics.start("resample")
        self.massive_table_sparse = self.massive_table_sparse.resample(self.param.granularity_secs).mean()
        self.metrics.end("resample")

        self.metrics.start("ffill")
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
        self.metrics.end("ffill")

        self.metrics.start("reindex")
        self.massive_table_filled.index = (
            self.massive_table_filled.index.drop_duplicates(keep="first")
        )
        self.metrics.end("reindex")

    def _merge_row(self, new_row):
        index = new_row.index[0]
        for column_name in list(new_row.keys()):
            value = new_row[column_name].array[0]

            self.massive_table_sparse.loc[index][column_name] = value

        self.metrics.start("ffill")
        self.massive_table_filled = self.massive_table_sparse.ffill()
        self.metrics.end("ffill")

    def merge_data(self, new_data):
        def combiner(existing, newer):
            existing_values = (
                existing.values if hasattr(existing, "values") else existing
            )
            newer_values = newer.values if hasattr(newer, "values") else newer

            # use newer values if they aren't nan
            condition = pd.isnull(newer_values)

            return expressions.where(condition, existing_values, newer_values)

        if len(new_data) == 0:
            return

        if len(new_data) == 1 and new_data.index[0] in self.massive_table_sparse.index:
            self.metrics.start("merge_row")
            self._merge_row(new_data)
            self.metrics.end("merge_row")
            return

        self.metrics.start("combine")
        self.massive_table_sparse = self.massive_table_sparse.combine(
            new_data, combiner
        )
        self.metrics.end("combine")

        self._fill_table()

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

    def get_current_window(self) -> pd.DataFrame:
        # This will get the nearest previous index that matches the timestamp,
        # so we don't need to specify the timestamps exactly
        start_index = self.massive_table_filled.index.get_loc(self.current_time, "ffill")
        end_index = self.massive_table_filled.index.get_loc(self.current_time + self.param.interval_secs, "ffill")
        return (
            self.massive_table_filled.iloc[start_index:start_index + 1]
            if self.get_window_span() == 1 else
            self.massive_table_filled.iloc[start_index:end_index])

    def get_window_at(self, time):
        start_index = None
        end_index = None

        # If we only have a single row, use it
        if self.massive_table_filled.shape[0] == 1:
            start_index = self.massive_table_filled.index.get_loc(time)
            end_index = start_index
        else:
            start_index = self.massive_table_filled.index.get_loc(
                time - self.param.interval_secs, "ffill"
            )
            end_index = self.massive_table_filled.index.get_loc(time, "ffill")

        if self.get_window_span() == 1:
            return self.massive_table_filled.iloc[start_index:start_index + 1]

        return self.massive_table_filled.iloc[start_index:end_index]

    def reset(self):
        self.current_time = self.param.epoch_time

    def advance(self) -> bool:
        if self.current_time >= self.param.end_time - self.param.interval_secs:
            return False

        self.current_time += self.param.granularity_secs

        return True
