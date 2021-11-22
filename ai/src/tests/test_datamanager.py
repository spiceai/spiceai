from io import StringIO
from typing import Dict
import unittest

import pandas as pd

from data_manager.base_manager import DataParam
from data_manager.time_series_manager import TimeSeriesDataManager
from proto.aiengine.v1 import aiengine_pb2


def get_test_fields(fill_method) -> Dict[str, aiengine_pb2.FieldData]:
    return {
        "foo": aiengine_pb2.FieldData(
            initializer=10.0, fill_method=fill_method
        ),
        "bar": aiengine_pb2.FieldData(
            initializer=0.0, fill_method=fill_method
        )
    }


def get_test_data_manager(fill_method=aiengine_pb2.FILL_ZERO, fields=None):
    if fields is None:
        fields = get_test_fields(fill_method)
    return TimeSeriesDataManager(
        param=DataParam(
            epoch_time=pd.to_datetime(10, unit="s"),
            period_secs=pd.to_timedelta(1, unit="s"),
            interval_secs=pd.to_timedelta(1, unit="s"),
            granularity_secs=pd.to_timedelta(10, unit="s")),
        fields=fields,
        action_rewards={"foo": "bar"},
        actions_order={"foo": 0},
        laws=["law"],
        external_reward_funcs="",
    )


class TimeSeriesDataManagerTestCase(unittest.TestCase):
    def test_column_order_changing(self):
        reversed_fields = {
            "bar": aiengine_pb2.FieldData(
                initializer=0.0, fill_method=aiengine_pb2.FILL_FORWARD
            ),
            "foo": aiengine_pb2.FieldData(
                initializer=10.0, fill_method=aiengine_pb2.FILL_FORWARD
            )
        }

        normal_data_manager = get_test_data_manager(fill_method=aiengine_pb2.FILL_FORWARD)

        original_csv = "time,foo\n20,2.0\n30,3.0\n50,4.0"
        original_data = pd.read_csv(StringIO(original_csv))
        original_data["time"] = pd.to_datetime(original_data["time"], unit="s")
        original_data = original_data.set_index("time")

        normal_data_manager.merge_data(original_data)

        reversed_data_manager = get_test_data_manager(fields=reversed_fields)
        reversed_data_manager.merge_data(original_data)

        window_index = normal_data_manager.massive_table_sparse.last_valid_index()

        normal_window = normal_data_manager.get_window_at(window_index)
        normal_flattened = normal_data_manager.flatten_and_normalize_window(normal_window)
        reversed_window = reversed_data_manager.get_window_at(window_index)
        reversed_flattened = reversed_data_manager.flatten_and_normalize_window(reversed_window)

        self.assertEqual(len(normal_flattened), len(reversed_flattened))

        for i, _ in enumerate(normal_flattened):
            self.assertEqual(normal_flattened[i], reversed_flattened[i])

    def test_zero_fill_data(self):
        data_manager = get_test_data_manager(fill_method=aiengine_pb2.FILL_ZERO)

        # Leave a gap at time 40
        original_csv = "time,foo\n10,1.0\n20,2.0\n30,3.0\n50,4.0"
        original_data = pd.read_csv(StringIO(original_csv))
        original_data["time"] = pd.to_datetime(original_data["time"], unit="s")
        original_data = original_data.set_index("time")

        data_manager.merge_data(original_data)
        filled_table = data_manager._fill_table(data_manager.massive_table_sparse)  # pylint: disable=protected-access

        expected_data = {10: 1.0, 20: 2.0, 30: 3.0, 40: 0.0, 50: 4.0}

        for key, value in expected_data.items():
            self.assertEqual(value, filled_table["foo"].loc[pd.to_datetime(key, unit="s")])

    def test_initial_data_preserved(self):
        data_manager = get_test_data_manager(fill_method=aiengine_pb2.FILL_ZERO)

        # Leave a gap at time 40
        original_csv = "time,foo\n20,2.0\n30,3.0\n50,4.0"
        original_data = pd.read_csv(StringIO(original_csv))
        original_data["time"] = pd.to_datetime(original_data["time"], unit="s")
        original_data = original_data.set_index("time")

        data_manager.merge_data(original_data)
        filled_table = data_manager._fill_table(data_manager.massive_table_sparse)  # pylint: disable=protected-access

        expected_data = {10: 10.0, 20: 2.0, 30: 3.0, 40: 0.0, 50: 4.0}

        for key, value in expected_data.items():
            self.assertEqual(value, filled_table["foo"].loc[pd.to_datetime(key, unit="s")])

    def test_new_data_out_of_order(self):
        data_manager = get_test_data_manager(fill_method=aiengine_pb2.FILL_FORWARD)

        # Leave a gap at time 40
        original_csv = "time,foo\n40,2.0\n30,3.0\n10,4.0"
        original_data = pd.read_csv(StringIO(original_csv))
        original_data["time"] = pd.to_datetime(original_data["time"], unit="s")
        original_data = original_data.set_index("time")

        data_manager.merge_data(original_data)
        filled_table = data_manager._fill_table(data_manager.massive_table_sparse)  # pylint: disable=protected-access

        expected_data = {10: 4.0, 20: 4.0, 30: 3.0, 40: 2.0}

        for key, value in expected_data.items():
            self.assertEqual(value, filled_table["foo"].loc[pd.to_datetime(key, unit="s")])

    def test_forward_fill_data(self):
        data_manager = get_test_data_manager(fill_method=aiengine_pb2.FILL_FORWARD)

        # Leave a gap at time 40
        original_csv = "time,foo\n10,1.0\n20,2.0\n30,3.0\n50,4.0"
        original_data = pd.read_csv(StringIO(original_csv))
        original_data["time"] = pd.to_datetime(original_data["time"], unit="s")
        original_data = original_data.set_index("time")

        data_manager.merge_data(original_data)
        filled_table = data_manager._fill_table(data_manager.massive_table_sparse)  # pylint: disable=protected-access

        expected_data = {10: 1.0, 20: 2.0, 30: 3.0, 40: 3.0, 50: 4.0}

        for key, value in expected_data.items():
            self.assertEqual(value, filled_table["foo"].loc[pd.to_datetime(key, unit="s")])

    def test_merge_data_resampling(self):
        data_manager = get_test_data_manager(fill_method=aiengine_pb2.FILL_FORWARD)

        original_csv = "time,foo\n10,1.0\n20,2.0\n30,3.0"
        original_data = pd.read_csv(StringIO(original_csv))
        original_data["time"] = pd.to_datetime(original_data["time"], unit="s")
        original_data = original_data.set_index("time")

        data_manager.merge_data(original_data)

        new_csv = "time,foo\n15,3.0\n25,4.0\n35,5.0"
        new_data = pd.read_csv(StringIO(new_csv))
        new_data["time"] = pd.to_datetime(new_data["time"], unit="s")
        new_data = new_data.set_index("time")

        data_manager.merge_data(new_data)

        self.assertEqual(data_manager.massive_table_sparse.shape, (3, 2))
        self.assertEqual(data_manager.massive_table_sparse["foo"][0], 2.0)

    def test_get_shape(self):

        test_cases = (
            {
                "fields": {"foo": aiengine_pb2.FieldData(initializer=0.0)},
                "expected_shape": (10,),
            },
            {
                "fields": {
                    "foo": aiengine_pb2.FieldData(initializer=0.0),
                    "foo2": aiengine_pb2.FieldData(initializer=0.0),
                },
                "expected_shape": (20,),
            },
            {
                "fields": {
                    "foo": aiengine_pb2.FieldData(initializer=0.0),
                    "foo2": aiengine_pb2.FieldData(initializer=0.0),
                    "foo3": aiengine_pb2.FieldData(initializer=0.0),
                },
                "expected_shape": (30,),
            },
        )

        for test_case in test_cases:
            data_manager = TimeSeriesDataManager(
                param=DataParam(
                    epoch_time=pd.to_datetime(10, unit="s"),
                    period_secs=pd.to_timedelta(1, unit="s"),
                    interval_secs=pd.to_timedelta(100, unit="s"),
                    granularity_secs=pd.to_timedelta(10, unit="s")),
                fields=test_case["fields"],
                action_rewards={"foo": "bar"},
                actions_order={"foo": 0},
                laws=["law"],
                external_reward_funcs="",
            )

            self.assertEqual(
                data_manager.get_shape(), test_case["expected_shape"], test_case
            )

    def test_get_window_span(self):
        test_cases = (
            {"interval": 100, "granularity": 10, "expected_window_span": 10},
            {"interval": 60, "granularity": 10, "expected_window_span": 6},
            {"interval": 200, "granularity": 10, "expected_window_span": 20},
            {"interval": 100, "granularity": 1, "expected_window_span": 100},
            {"interval": 35, "granularity": 10, "expected_window_span": 3},
        )

        for test_case in test_cases:
            data_manager = TimeSeriesDataManager(
                param=DataParam(
                    epoch_time=pd.to_datetime(10, unit="s"),
                    period_secs=pd.to_timedelta(1, unit="s"),
                    interval_secs=pd.to_timedelta(test_case["interval"], unit="s"),
                    granularity_secs=pd.to_timedelta(test_case["granularity"], unit="s")),
                fields={"foo": aiengine_pb2.FieldData(initializer=0.0)},
                action_rewards={"foo": "bar"},
                actions_order={"foo": 0},
                laws=["law"],
                external_reward_funcs="",
            )

            self.assertEqual(
                data_manager.get_window_span(),
                test_case["expected_window_span"],
                test_case,
            )

    def test_reset(self):
        epoch_time = pd.to_datetime(10, unit="s")
        interval = pd.to_timedelta(100, unit="s")
        data_manager = TimeSeriesDataManager(
            param=DataParam(
                epoch_time=epoch_time,
                period_secs=pd.to_timedelta(1, unit="s"),
                interval_secs=interval,
                granularity_secs=pd.to_timedelta(10, unit="s")),
            fields={"foo": aiengine_pb2.FieldData(initializer=0.0)},
            action_rewards={"foo": "bar"},
            actions_order={"foo": 0},
            laws=["law"],
            external_reward_funcs="",
        )

        self.assertIsNone(data_manager.current_time)
        data_manager.reset()
        self.assertEqual(data_manager.current_time, epoch_time)

    def test_advance(self):
        epoch_time = pd.to_datetime(0, unit="s")
        period = pd.to_timedelta(20, unit="s")
        interval = pd.to_timedelta(10, unit="s")
        granularity = pd.to_timedelta(1, unit="s")
        data_manager = TimeSeriesDataManager(
            param=DataParam(
                epoch_time=epoch_time,
                period_secs=period,
                interval_secs=interval,
                granularity_secs=granularity),
            fields={"foo": aiengine_pb2.FieldData(initializer=0.0)},
            action_rewards={"foo": "bar"},
            actions_order={"foo": 0},
            laws=["law"],
            external_reward_funcs="",
        )

        original_csv = "time,foo\n10,1.0\n20,2.0\n30,3.0"
        original_data = pd.read_csv(StringIO(original_csv))
        original_data["time"] = pd.to_datetime(original_data["time"], unit="s")
        original_data = original_data.set_index("time")

        data_manager.merge_data(original_data)

        data_manager.reset()

        expected_current_time = epoch_time
        self.assertEqual(data_manager.current_time, expected_current_time)

        expected_steps = int((period - interval) / granularity)
        step = 0
        while True:
            if expected_steps == step:
                self.assertFalse(data_manager.advance(), step)

                # Should not move beyond epoch + period (20s in this case)
                self.assertEqual(data_manager.current_time, expected_current_time)
                break

            self.assertTrue(data_manager.advance(), step)
            expected_current_time += granularity
            self.assertEqual(data_manager.current_time, expected_current_time)
            step += 1


if __name__ == "__main__":
    unittest.main()
