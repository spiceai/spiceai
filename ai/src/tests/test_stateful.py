import unittest
from io import StringIO
import pandas as pd
import numpy as np
from connector.stateful import StatefulConnector
from data import DataManager

original_csv = "time,baz\n10,1.0\n20,2.0\n30,3.0\n40,4.0\n50,5.0"
original_data = pd.read_csv(StringIO(original_csv))
original_data["time"] = pd.to_datetime(original_data["time"], unit="s")
original_data = original_data.set_index("time")

granularity = pd.to_timedelta(10, unit="s")
epoch_time = pd.to_datetime(10, unit="s")
period = pd.to_timedelta(50, unit="s")
interval = pd.to_timedelta(20, unit="s")


class StatefulConnectorTests(unittest.TestCase):
    def setUp(self):
        self.data_manager = DataManager()

        self.data_manager.init(
            epoch_time=epoch_time,
            period_secs=period,
            interval_secs=interval,
            granularity_secs=granularity,
            fields={"foo": 10.0, "bar": 5.0},
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

        self.data_manager.merge_data(original_data)
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
        indexToCheck = pd.to_datetime(30, unit="s")
        expectedBar = 4.0
        expectedFoo = 15.0
        actualBar = self.data_manager.massive_table_sparse.loc[indexToCheck]["bar"]
        actualFoo = self.data_manager.massive_table_sparse.loc[indexToCheck]["foo"]
        self.assertEqual(expectedBar, actualBar)
        self.assertEqual(expectedFoo, actualFoo)

        self.assertTrue(
            np.isnan(
                self.data_manager.massive_table_sparse.loc[indexToCheck + granularity][
                    "bar"
                ]
            )
        )
        self.assertEqual(
            expectedBar,
            self.data_manager.massive_table_filled.loc[indexToCheck + granularity][
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
        indexToCheck = pd.to_datetime(30, unit="s")
        actualBar = self.data_manager.massive_table_sparse.loc[indexToCheck]["bar"]
        actualFoo = self.data_manager.massive_table_sparse.loc[indexToCheck]["foo"]
        self.assertTrue(np.isnan(actualBar))
        self.assertTrue(np.isnan(actualFoo))

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
