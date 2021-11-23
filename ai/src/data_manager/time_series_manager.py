import math
from typing import Dict, List

import numpy as np
import pandas as pd

from data_manager.base_manager import DataManagerBase, DataParam
from proto.aiengine.v1 import aiengine_pb2


class TimeSeriesDataManager(DataManagerBase):
    def __init__(self, param: DataParam, fields: Dict[str, aiengine_pb2.FieldData], action_rewards: Dict[str, str],
                 actions_order: Dict[str, int], external_reward_funcs: str, laws: List[str]):
        super().__init__(param, fields, action_rewards, actions_order, external_reward_funcs, laws)

        new_series = {}
        sorted_field_names = sorted(fields)
        for field_name in sorted_field_names:
            new_series[field_name] = [fields[field_name].initializer]

        self.massive_table_sparse = pd.DataFrame(new_series, index={self.param.epoch_time})
        self.massive_table_training_filled = None

        self.current_time: pd.Timestamp = None
        self.is_training = False

    def get_window_span(self):
        return math.floor(self.param.interval_secs / self.param.granularity_secs)

    def start_training(self):
        if self.is_training:
            raise Exception("unable to start a new training run before the previous has finished")
        self.is_training = True
        self.metrics.start("copy_training_table")
        self.massive_table_training_filled = self._fill_table(self.massive_table_sparse)
        self.metrics.end("copy_training_table")

    def end_training(self):
        self.is_training = False
        self.massive_table_training_filled = None

    def _resample_table(self, table_to_resample: pd.DataFrame) -> pd.DataFrame:
        self.metrics.start("resample")
        resampled_table = table_to_resample.resample(self.param.granularity_secs).mean()
        self.metrics.end("resample")
        return resampled_table

    def _fill_table(self, input_table: pd.DataFrame) -> pd.DataFrame:
        table_to_fill = input_table.copy()
        self.metrics.start("ffill")
        for col_name in table_to_fill:
            fill_method = self.fields[col_name].fill_method
            if fill_method == aiengine_pb2.FILL_FORWARD:
                table_to_fill[col_name] = table_to_fill[
                    col_name
                ].ffill()
            elif fill_method == aiengine_pb2.FILL_ZERO:
                table_to_fill[col_name] = table_to_fill[
                    col_name
                ].fillna(0)
        self.metrics.end("ffill")
        return table_to_fill

    def merge_training_row(self, new_row: pd.DataFrame):
        if not self.is_training:
            raise Exception("only valid to call merge_training_row during a training run")
        index = new_row.index[0]
        for column_name in list(new_row.keys()):
            value = new_row[column_name].array[0]

            self.massive_table_training_filled.loc[index][column_name] = value

    def merge_data(self, new_data: pd.DataFrame):
        if len(new_data) == 0:
            return

        new_data_resampled = self._resample_table(new_data)

        # On initial data load, overwrite initializers if we have actual data for them
        if len(self.massive_table_sparse) == 1 and self.massive_table_sparse.index[0] == new_data_resampled.index[0]:
            initial_row = self.massive_table_sparse.iloc[0]
            for key, val in new_data_resampled.iloc[0].iteritems():
                initial_row[key] = val

        self.metrics.start("concat")
        concat_table = pd.concat([self.massive_table_sparse, new_data_resampled])
        self.metrics.end("concat")
        self.massive_table_sparse = self._resample_table(concat_table)

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

    # This method should only be called during training.
    def get_current_window(self) -> pd.DataFrame:
        if not self.is_training:
            raise Exception("Start training before calling get_current_window()")

        # This will get the nearest previous index that matches the timestamp,
        # so we don't need to specify the timestamps exactly
        start_index = self.massive_table_training_filled.index.get_loc(self.current_time, "ffill")
        end_index = self.massive_table_training_filled.index.get_loc(
            self.current_time + self.param.interval_secs, "ffill")
        return (
            self.massive_table_training_filled.iloc[start_index:start_index + 1]
            if self.get_window_span() == 1 else
            self.massive_table_training_filled.iloc[start_index:end_index])

    def get_window_at(self, time: pd.Timestamp):
        start_index = None
        end_index = None

        filled_table = self._fill_table(self.massive_table_sparse)

        # If we only have a single row, use it
        if filled_table.shape[0] == 1:
            start_index = filled_table.index.get_loc(time)
            end_index = start_index
        else:
            start_index = filled_table.index.get_loc(
                time - self.param.interval_secs, "ffill"
            )
            end_index = filled_table.index.get_loc(time, "ffill")

        if self.get_window_span() == 1:
            return filled_table.iloc[start_index:start_index + 1]

        return filled_table.iloc[start_index:end_index]

    def reset(self):
        self.current_time = self.param.epoch_time

    def advance(self) -> bool:
        if self.current_time >= self.param.end_time - self.param.interval_secs:
            return False

        self.current_time += self.param.granularity_secs

        return True
