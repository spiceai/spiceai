from abc import ABC, abstractmethod

import pandas as pd
import numpy as np


class DataParam:
    def __init__(self, epoch_time: pd.Timestamp, period_secs: pd.Timedelta, interval_secs: pd.Timedelta,
                 granularity_secs: pd.Timedelta):
        self.interval_secs = interval_secs
        self.granularity_secs = granularity_secs
        self.epoch_time = epoch_time
        self.period_secs = period_secs
        self.end_time = epoch_time + self.period_secs


class DataManagerBase(ABC):
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

    @abstractmethod
    def add_interpretations(self, interpretations):
        raise RuntimeError('Not Implemented')

    @abstractmethod
    def get_current_window(self) -> pd.DataFrame:
        raise RuntimeError('Not Implemented')

    @abstractmethod
    def get_shape(self) -> tuple:
        raise RuntimeError('Not Implemented')

    @abstractmethod
    def merge_data(self, new_data):
        raise RuntimeError('Not Implemented')

    @abstractmethod
    def advance(self) -> bool:
        raise RuntimeError('Not Implemented')

    @abstractmethod
    def reset(self):
        raise RuntimeError('Not Implemented')

    @abstractmethod
    def reward(
            self, prev_state_pd, prev_state_interpretations,
            new_state_pd, new_state_intepretations, action: int) -> float:
        raise RuntimeError('Not Implemented')
