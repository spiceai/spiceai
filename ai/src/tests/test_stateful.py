from io import StringIO
import unittest

import pandas as pd
import numpy as np

from connector.stateful import StatefulConnector
from data import DataParam, DataManager
from proto.aiengine.v1 import aiengine_pb2


class StatefulConnectorTests(unittest.TestCase):
    def __init__(self, method_name='runTest'):
        super().__init__(method_name)

        self.original_csv = "time,baz\n10,1.0\n20,2.0\n30,3.0\n40,4.0\n50,5.0"
        self.original_data = pd.read_csv(StringIO(self.original_csv))
        self.original_data["time"] = pd.to_datetime(self.original_data["time"], unit="s")
        self.original_data = self.original_data.set_index("time")

        self.granularity = pd.to_timedelta(10, unit="s")
        self.epoch_time = pd.to_datetime(10, unit="s")
        self.period = pd.to_timedelta(50, unit="s")
        self.interval = pd.to_timedelta(20, unit="s")

    def setUp(self):
        self.data_manager = DataManager(
            param=DataParam(
                epoch_time=self.epoch_time,
                period_secs=self.period,
                interval_secs=self.interval,
                granularity_secs=self.granularity),
            fields={
                "foo": aiengine_pb2.FieldData(initializer=10.0),
                "bar": aiengine_pb2.FieldData(initializer=5.0),
                "baz": aiengine_pb2.FieldData(initializer=1.0),
            },
            action_rewards={
                "foo_action": "reward = 1",
                "bar_action": "reward = 1",
            },
            actions_order={
                "foo_action": 0,
                "bar_action": 1,
            },
            laws=["bar >= 0"],
        )

        self.data_manager.merge_data(self.original_data)
        self.data_manager.rewind()

    def test_apply_action(self):
        action_effects = {
            "foo_action": "foo += 5\nbar -= 1",
            "bar_action": "foo += baz",
        }
        stateful_connector = StatefulConnector(self.data_manager, action_effects)

        current_window = self.data_manager.get_current_window()

        is_valid = stateful_connector.apply_action(0, current_window)
        self.assertTrue(is_valid)
        index_to_check = pd.to_datetime(30, unit="s")
        expected_bar = 4.0
        expected_foo = 15.0
        actual_bar = self.data_manager.massive_table_sparse.loc[index_to_check]["bar"]
        actual_foo = self.data_manager.massive_table_sparse.loc[index_to_check]["foo"]
        self.assertEqual(expected_bar, actual_bar)
        self.assertEqual(expected_foo, actual_foo)

        self.assertTrue(
            np.isnan(
                self.data_manager.massive_table_sparse.loc[index_to_check + self.granularity][
                    "bar"
                ]
            )
        )
        self.assertEqual(
            expected_bar,
            self.data_manager.massive_table_filled.loc[index_to_check + self.granularity][
                "bar"
            ],
        )

    def test_laws(self):
        action_effects = {
            "foo_action": "foo += 5\nbar -= 10",
            "bar_action": "foo += baz",
        }
        stateful_connector = StatefulConnector(self.data_manager, action_effects)

        current_window = self.data_manager.get_current_window()

        # This should not be valid and not apply the update
        is_valid = stateful_connector.apply_action(0, current_window)
        self.assertFalse(is_valid)
        index_to_check = pd.to_datetime(30, unit="s")
        actual_bar = self.data_manager.massive_table_sparse.loc[index_to_check]["bar"]
        actual_foo = self.data_manager.massive_table_sparse.loc[index_to_check]["foo"]
        self.assertTrue(np.isnan(actual_bar))
        self.assertTrue(np.isnan(actual_foo))

    def test_is_calling_merge_row(self):
        original_fill_table = self.data_manager.fill_table

        def new_fill_table():
            raise Exception("Should not call this on apply_action")

        try:
            self.data_manager.fill_table = new_fill_table

            action_effects = {
                "foo_action": "foo += 5\nbar -= 1",
                "bar_action": "foo += baz",
            }
            stateful_connector = StatefulConnector(self.data_manager, action_effects)

            current_window = self.data_manager.get_current_window()

            is_valid = stateful_connector.apply_action(0, current_window)
            self.assertTrue(is_valid)
        finally:
            self.data_manager.fill_table = original_fill_table


if __name__ == "__main__":
    unittest.main()
