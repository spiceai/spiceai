from io import StringIO
import unittest

import pandas as pd

from data import DataParam, DataManager
from proto.aiengine.v1 import aiengine_pb2


class DataManagerTestCase(unittest.TestCase):
    def test_zero_fill_data(self):
        data_manager = DataManager(
            param=DataParam(
                epoch_time=pd.to_datetime(10, unit="s"),
                period_secs=pd.to_timedelta(1, unit="s"),
                interval_secs=pd.to_timedelta(1, unit="s"),
                granularity_secs=pd.to_timedelta(10, unit="s")),
            fields={
                "foo": aiengine_pb2.FieldData(
                    initializer=10.0, fill_method=aiengine_pb2.FILL_ZERO
                )
            },
            action_rewards={"foo": "bar"},
            actions_order={"foo": 0},
            laws=["law"],
        )

        # Leave a gap at time 40
        original_csv = "time,foo\n10,1.0\n20,2.0\n30,3.0\n50,4.0"
        original_data = pd.read_csv(StringIO(original_csv))
        original_data["time"] = pd.to_datetime(original_data["time"], unit="s")
        original_data = original_data.set_index("time")

        data_manager.merge_data(original_data)

        expected_data = {10: 1.0, 20: 2.0, 30: 3.0, 40: 0.0, 50: 4.0}

        for key, value in expected_data.items():
            self.assertEqual(value, data_manager.massive_table_filled.loc[pd.to_datetime(key, unit="s")].values[0])

    def test_forward_fill_data(self):
        data_manager = DataManager(
            param=DataParam(
                epoch_time=pd.to_datetime(10, unit="s"),
                period_secs=pd.to_timedelta(1, unit="s"),
                interval_secs=pd.to_timedelta(1, unit="s"),
                granularity_secs=pd.to_timedelta(10, unit="s")),
            fields={
                "foo": aiengine_pb2.FieldData(
                    initializer=10.0, fill_method=aiengine_pb2.FILL_FORWARD
                )
            },
            action_rewards={"foo": "bar"},
            actions_order={"foo": 0},
            laws=["law"],
        )

        # Leave a gap at time 40
        original_csv = "time,foo\n10,1.0\n20,2.0\n30,3.0\n50,4.0"
        original_data = pd.read_csv(StringIO(original_csv))
        original_data["time"] = pd.to_datetime(original_data["time"], unit="s")
        original_data = original_data.set_index("time")

        data_manager.merge_data(original_data)

        expected_data = {10: 1.0, 20: 2.0, 30: 3.0, 40: 3.0, 50: 4.0}

        for key, value in expected_data.items():
            self.assertEqual(value, data_manager.massive_table_filled.loc[pd.to_datetime(key, unit="s")].values[0])

    def test_merge_data_resampling(self):
        data_manager = DataManager(
            param=DataParam(
                epoch_time=pd.to_datetime(10, unit="s"),
                period_secs=pd.to_timedelta(1, unit="s"),
                interval_secs=pd.to_timedelta(1, unit="s"),
                granularity_secs=pd.to_timedelta(10, unit="s")),
            fields={"foo": aiengine_pb2.FieldData(initializer=0.0)},
            action_rewards={"foo": "bar"},
            actions_order={"foo": 0},
            laws=["law"],
        )

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

        self.assertEqual(data_manager.massive_table_filled.shape, (3, 1))
        self.assertEqual(data_manager.massive_table_filled["foo"][0], 2.0)

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
            data_manager = DataManager(
                param=DataParam(
                    epoch_time=pd.to_datetime(10, unit="s"),
                    period_secs=pd.to_timedelta(1, unit="s"),
                    interval_secs=pd.to_timedelta(100, unit="s"),
                    granularity_secs=pd.to_timedelta(10, unit="s")),
                fields=test_case["fields"],
                action_rewards={"foo": "bar"},
                actions_order={"foo": 0},
                laws=["law"],
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
            data_manager = DataManager(
                param=DataParam(
                    epoch_time=pd.to_datetime(10, unit="s"),
                    period_secs=pd.to_timedelta(1, unit="s"),
                    interval_secs=pd.to_timedelta(test_case["interval"], unit="s"),
                    granularity_secs=pd.to_timedelta(test_case["granularity"], unit="s")),
                fields={"foo": aiengine_pb2.FieldData(initializer=0.0)},
                action_rewards={"foo": "bar"},
                actions_order={"foo": 0},
                laws=["law"],
            )

            self.assertEqual(
                data_manager.get_window_span(),
                test_case["expected_window_span"],
                test_case,
            )

    def test_rewind(self):
        epoch_time = pd.to_datetime(10, unit="s")
        interval = pd.to_timedelta(100, unit="s")
        data_manager = DataManager(
            param=DataParam(
                epoch_time=epoch_time,
                period_secs=pd.to_timedelta(1, unit="s"),
                interval_secs=interval,
                granularity_secs=pd.to_timedelta(10, unit="s")),
            fields={"foo": aiengine_pb2.FieldData(initializer=0.0)},
            action_rewards={"foo": "bar"},
            actions_order={"foo": 0},
            laws=["law"],
        )

        self.assertIsNone(data_manager.current_time)
        data_manager.rewind()
        expected_current_time = epoch_time + interval
        self.assertEqual(data_manager.current_time, expected_current_time)

    def test_advance(self):
        epoch_time = pd.to_datetime(0, unit="s")
        period = pd.to_timedelta(20, unit="s")
        interval = pd.to_timedelta(10, unit="s")
        granularity = pd.to_timedelta(1, unit="s")
        data_manager = DataManager(
            param=DataParam(
                epoch_time=epoch_time,
                period_secs=period,
                interval_secs=interval,
                granularity_secs=granularity),
            fields={"foo": aiengine_pb2.FieldData(initializer=0.0)},
            action_rewards={"foo": "bar"},
            actions_order={"foo": 0},
            laws=["law"],
        )

        original_csv = "time,foo\n10,1.0\n20,2.0\n30,3.0"
        original_data = pd.read_csv(StringIO(original_csv))
        original_data["time"] = pd.to_datetime(original_data["time"], unit="s")
        original_data = original_data.set_index("time")

        data_manager.merge_data(original_data)

        data_manager.rewind()

        expected_current_time = epoch_time + interval
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
