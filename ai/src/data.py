import numpy as np
import pandas as pd
import pandas.core.computation.expressions as expressions
from types import SimpleNamespace
import math
import threading
from exception import RewardInvalidException
from metrics import metrics


class DataManager:
    def init(
        self,
        epoch_time: pd.Timestamp,
        period_secs: pd.Timedelta,
        interval_secs: pd.Timedelta,
        granularity_secs: pd.Timedelta,
        fields: "dict[str]",
        action_rewards: "dict[str]",
        laws: "list[str]",
    ):
        self.fields = fields
        self.laws = laws
        self.interval_secs = interval_secs
        self.granularity_secs = granularity_secs
        self.epoch_time = epoch_time
        self.period_secs = period_secs
        self.end_time = epoch_time + self.period_secs

        new_series = dict()
        for field_name in fields:
            new_series[field_name] = [fields[field_name]]

        self.massive_table_sparse = pd.DataFrame(new_series, index={self.epoch_time})
        self.massive_table_sparse = self.massive_table_sparse.resample(
            self.granularity_secs
        ).mean()
        self.fill_table()

        self.current_time: pd.Timestamp = None
        self.action_rewards = action_rewards
        self.action_names = list(action_rewards.keys())
        self.table_lock = threading.Lock()

    def get_window_span(self):
        return math.floor(self.interval_secs / self.granularity_secs)

    def overwrite_data(self, new_data):
        self.massive_table_sparse = new_data

        self.fill_table()

    def fill_table(self):
        metrics.start("resample")
        self.massive_table_sparse = self.massive_table_sparse.resample(
            self.granularity_secs
        ).mean()
        metrics.end("resample")

        metrics.start("ffill")
        self.massive_table_filled = self.massive_table_sparse.ffill()
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
            if (
                len(new_data) == 1
                and new_data.index[0] in self.massive_table_sparse.index
            ):
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

    def get_shape(self):
        return np.shape([0] * self.get_window_span() * len(self.fields))

    def flatten_and_normalize_window(self, current_window):
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
            # This will get the nearest previous index that matches the timestamp, so we don't need to specify the timestamps exactly
            start_index = self.massive_table_filled.index.get_loc(
                self.current_time - self.interval_secs, "ffill"
            )
            end_index = self.massive_table_filled.index.get_loc(
                self.current_time, "ffill"
            )

            if self.get_window_span() == 1:
                return self.massive_table_filled.iloc[start_index : start_index + 1]
            else:
                return self.massive_table_filled.iloc[start_index:end_index]

    def get_latest_window(self):
        latest_time = self.massive_table_filled.last_valid_index()
        start_index = self.massive_table_filled.index.get_loc(
            latest_time - self.interval_secs, "ffill"
        )
        end_index = self.massive_table_filled.index.get_loc(latest_time, "ffill")
        if self.get_window_span() == 1:
            return self.massive_table_filled.iloc[start_index : start_index + 1]
        else:
            return self.massive_table_filled.iloc[start_index:end_index]

    def rewind(self):
        self.current_time = self.epoch_time + self.interval_secs

    def advance(self):
        if self.current_time >= self.end_time:
            return False

        self.current_time += self.granularity_secs

        return True

    def reward(self, prev_state_pd, new_state_pd, action: int):
        prev_state_dict = dict()
        new_state_dict = dict()

        for key in prev_state_pd:
            prev_state_dict[key] = list(prev_state_pd[key])[-1]
            new_state_dict[key] = list(new_state_pd[key])[-1]

        prev_state = SimpleNamespace(**prev_state_dict)
        new_state = SimpleNamespace(**new_state_dict)

        loc = dict()
        loc["prev_state"] = prev_state
        loc["new_state"] = new_state

        action_name = self.action_names[action]
        reward_func = self.action_rewards[action_name]

        try:
            exec(reward_func, globals(), loc)
        except Exception as ex:
            raise RewardInvalidException(repr(ex))

        return loc["reward"]
