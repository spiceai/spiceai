from data import DataManager
import gym
import numpy as np
import pandas as pd
from exception import InvalidFieldsException, UnsupportedGymEnvironmentException

environment_fields = {
    "CartPole-v1": [
        "cart_position",
        "cart_velocity",
        "pole_angle",
        "pole_angular_velocity",
        "is_done",
    ]
}


def get_field_order(environment_name: str, fields: "dict[str]") -> "dict[str]":
    if not environment_name in environment_fields:
        raise UnsupportedGymEnvironmentException(
            f"Unsupported gym environment '{environment_name}'"
        )

    expected_fields = environment_fields[environment_name]
    if len(fields) != len(expected_fields):
        raise InvalidFieldsException(
            f"Expected {len(expected_fields)} fields, got {len(fields)}"
        )

    field_order = dict()
    for i in range(len(expected_fields)):
        for field in fields:
            if field.endswith(expected_fields[i]):
                field_order[i] = field

    if len(field_order) != len(expected_fields):
        raise InvalidFieldsException(
            f"Fields didn't match the expected set for {environment_name}"
        )

    return field_order


class OpenAIGymConnector:
    def __init__(
        self,
        environment_name: str,
        data_manager: DataManager,
        fields: "dict[str]",
    ):
        self.field_order = get_field_order(environment_name, fields)
        self.is_done_position = len(environment_fields[environment_name]) - 1
        self.env = gym.make(environment_name)
        self.data_manager = data_manager
        current_state = self.env.reset()
        self.epoch_time = data_manager.massive_table_sparse.index[0]

        self.update_state(self.epoch_time, current_state)

    def update_state(self, next_timestamp: pd.Timestamp, current_state):
        new_series = dict()
        for index in range(len(current_state)):
            new_series[self.field_order[index]] = [current_state[index]]
        new_data = pd.DataFrame(new_series, index={next_timestamp})

        self.data_manager.merge_data(new_data)

    def apply_action(self, action: int, data_row: pd.DataFrame) -> bool:
        prev_is_done = data_row[self.field_order[self.is_done_position]].array[0]
        if prev_is_done == 1:
            return True

        current_state, _, is_done, _ = self.env.step(action)
        is_done_val = 1 if is_done else 0
        current_state = np.append(current_state, is_done_val)

        if is_done:
            current_state = self.env.reset()
            current_state = np.append(current_state, is_done_val)
            self.update_state(self.epoch_time, current_state)

        next_timestamp = data_row.index[0] + self.data_manager.granularity_secs
        self.update_state(next_timestamp, current_state)

        return True
