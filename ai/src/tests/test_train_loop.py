import copy
import csv
import io
import os
import threading
import unittest

import pandas as pd

from cleanup import cleanup_on_shutdown
from proto.aiengine.v1 import aiengine_pb2
import main
from tests import common
import train


class TrainingLoopTests(unittest.TestCase):
    ALGORITHM = os.environ["ALGORITHM"]

    def setUp(self):
        # Preventing tensorflow verbose initialization
        os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"
        import tensorflow as tf  # pylint: disable=import-outside-toplevel

        # Eager execution is too slow to use, so disabling
        tf.compat.v1.disable_eager_execution()

        self.aiengine = main.AIEngine()

        self.trader_init_req = common.get_init_from_json(
            init_data_path="../../test/assets/aiengine/api/trader_init.json",
            pod_name="trader",
        )

        with open("../../test/assets/data/csv/trader.csv", "r") as trader_data:
            self.trader_data_csv = trader_data.read()

        self.episode_results = list()
        self.original_post_episode_result = train.post_episode_result
        train.post_episode_result = (
            lambda request_url, episode_data: self.episode_results.append(
                {"request_url": request_url, "episode_data": episode_data}
            )
        )
        self.original_end_of_episode = train.end_of_episode

    def tearDown(self):
        train.post_episode_result = self.original_post_episode_result
        train.end_of_episode = self.original_end_of_episode
        cleanup_on_shutdown()

    def init(
        self,
        init_req: aiengine_pb2.InitRequest,
        expected_error: bool = False,
        expected_result: str = "ok",
    ):
        resp = self.aiengine.Init(init_req, None)
        self.assertEqual(resp.error, expected_error)
        self.assertEqual(resp.result, expected_result)

    def add_data(self, pod_name: str, csv_data: str):
        resp = self.aiengine.AddData(
            aiengine_pb2.AddDataRequest(pod=pod_name, csv_data=csv_data), None
        )
        self.assertFalse(resp.error)

    def start_training(
        self,
        pod_name: str,
        flight: str = None,
        number_episodes: int = None,
        epoch_time: int = None,
        expected_error: bool = False,
        expected_result: str = "started_training",
    ):
        train_req = aiengine_pb2.StartTrainingRequest(
            pod=pod_name,
            number_episodes=number_episodes,
            flight=flight,
            epoch_time=epoch_time,
            learning_algorithm=self.ALGORITHM,
        )

        resp = self.aiengine.StartTraining(train_req, None)

        self.assertEqual(resp.error, expected_error)
        self.assertEqual(resp.result, expected_result)

    def wait_for_training(self):
        self.assertIsNotNone(main.training_thread)
        main.training_thread.join()

    def inference(self, pod_name: str, tag: str, assertion_on_response=None):
        resp = self.aiengine.GetInference(
            aiengine_pb2.InferenceRequest(pod=pod_name, tag=tag), None
        )
        self.assertFalse(resp.response.error)
        self.assertEqual(resp.tag, tag)

        if assertion_on_response is not None:
            assertion_on_response(resp)

    def validate_episode_data(
        self, pod_name, flight, number_episodes, num_actions, episode_results
    ):
        self.assertEqual(len(episode_results), number_episodes)
        index = episode_results[0]["episode_data"]["episode"]
        for episode_result in episode_results:
            episode_data = episode_result["episode_data"]
            self.assertEqual(
                episode_result["request_url"],
                f"http://localhost:8000/api/v0.1/pods/{pod_name}/training_runs/{flight}/episodes",
            )
            self.assertEqual(episode_data["episode"], index)
            self.assertTrue(episode_data["start"])
            self.assertTrue(episode_data["end"])
            self.assertTrue(episode_data["score"])

            actionsCount = 0
            for action_name in episode_data["actions_taken"]:
                actionsCount += episode_data["actions_taken"][action_name]
            self.assertEqual(actionsCount, num_actions)
            index += 1

    def test_train_inference_loop(self):
        # Step 1, init the pod
        self.init(self.trader_init_req)

        # Step 2, load the csv data
        self.add_data("trader", self.trader_data_csv)

        FLIGHT = "1"
        NUMBER_EPISODES = 10
        # Step 3, train
        self.start_training("trader", FLIGHT, NUMBER_EPISODES)

        self.wait_for_training()

        # Step 4, inference
        self.inference(
            "trader",
            "latest",
            lambda response: self.assertNotEqual(response.confidence, 0.0),
        )

        # Validate the episode data
        self.validate_episode_data(
            "trader",
            FLIGHT,
            NUMBER_EPISODES,
            num_actions=50,
            episode_results=self.episode_results,
        )

    def test_train_inference_loop_train_different_epoch(self):
        # Step 1, init the pod
        self.init(self.trader_init_req)

        # Step 2, load the csv data
        self.add_data("trader", self.trader_data_csv)

        FLIGHT = "1"
        NUMBER_EPISODES = 10
        # Step 3, train
        self.start_training("trader", FLIGHT, NUMBER_EPISODES, 1626697490)

        self.wait_for_training()

        # Step 4, inference
        self.inference(
            "trader",
            "latest",
            lambda response: self.assertNotEqual(response.confidence, 0.0),
        )

        # Validate the episode data
        self.validate_episode_data(
            "trader",
            FLIGHT,
            NUMBER_EPISODES,
            num_actions=49,
            episode_results=self.episode_results,
        )

    def test_train_gap_in_data(self):
        with open("./tests/assets/csv/training_loop_gap_0.csv", "r") as data:
            gap_data_0 = data.read()
        with open("./tests/assets/csv/training_loop_gap_1.csv", "r") as data:
            gap_data_1 = data.read()

        self.init(self.trader_init_req)
        self.add_data("trader", gap_data_0)
        self.add_data("trader", gap_data_1)

        FLIGHT = "1"
        NUMBER_EPISODES = 10
        self.start_training("trader", FLIGHT, NUMBER_EPISODES)

        self.wait_for_training()

        self.validate_episode_data(
            "trader",
            FLIGHT,
            NUMBER_EPISODES,
            num_actions=50,
            episode_results=self.episode_results,
        )

        gap_start = pd.to_datetime(1626697640, unit="s")
        gap_end = pd.to_datetime(1626697860, unit="s")
        table = main.data_managers["trader"].massive_table_filled
        price = list(table[gap_start:gap_start].coinbase_btcusd_close)[-1]

        # Validate the forward filling is working.
        current_time = gap_start
        while current_time < gap_end:
            next_price = list(table[current_time:current_time].coinbase_btcusd_close)[
                -1
            ]
            self.assertEqual(price, next_price)
            price = next_price
            current_time += pd.to_timedelta(self.trader_init_req.granularity, unit="s")

    def test_data_added_after_training_starts(self):
        with open("./tests/assets/csv/training_loop_gap_0.csv", "r") as data:
            gap_data_0 = data.read()
        with open("./tests/assets/csv/training_loop_gap_1.csv", "r") as data:
            gap_data_1 = data.read()

        self.init(self.trader_init_req)
        self.add_data("trader", gap_data_0)

        FLIGHT = "1"
        NUMBER_EPISODES = 10
        self.start_training("trader", FLIGHT, NUMBER_EPISODES)

        post_data_lock = threading.Lock()
        episode_5_lock = threading.Lock()
        episode_5_lock.acquire()

        def release_lock_on_episode_5(episode: int):
            if episode == 5 and episode_5_lock.locked():
                episode_5_lock.release()
                post_data_lock.acquire()

        train.end_of_episode = release_lock_on_episode_5

        # wait for episode 5
        post_data_lock.acquire()
        episode_5_lock.acquire()

        print("Posting gap_data_1")
        self.add_data("trader", gap_data_1)
        post_data_lock.release()

        self.wait_for_training()
        episode_5_lock.release()
        post_data_lock.release()

        self.validate_episode_data(
            "trader",
            FLIGHT,
            5,
            num_actions=10,
            episode_results=self.episode_results[0:5],
        )
        self.validate_episode_data(
            "trader",
            FLIGHT,
            5,
            num_actions=50,
            episode_results=self.episode_results[5:],
        )

    def test_epoch_earlier_than_data(self):
        self.init(self.trader_init_req)
        self.add_data("trader", self.trader_data_csv)
        self.start_training(
            "trader",
            "1",
            10,
            1626697400,
            expected_error=True,
            expected_result="epoch_time_invalid",
        )

    def test_epoch_offset_from_data(self):
        self.init(self.trader_init_req)
        self.add_data("trader", self.trader_data_csv)
        self.start_training(
            "trader",
            "1",
            1,
            1626697485,
            expected_error=False,
            expected_result="started_training",
        )
        self.wait_for_training()

    def test_epoch_after_latest_data(self):
        self.init(self.trader_init_req)
        self.add_data("trader", self.trader_data_csv)
        self.start_training(
            "trader",
            "1",
            10,
            1626699240,
            expected_error=True,
            expected_result="not_enough_data_for_training",
        )

    def test_not_enough_data_for_training_no_data(self):
        self.init(self.trader_init_req)
        self.start_training(
            "trader",
            "1",
            10,
            expected_error=True,
            expected_result="not_enough_data_for_training",
        )

    def test_not_enough_data_for_training_late_epoch(self):
        self.init(self.trader_init_req)
        self.add_data("trader", self.trader_data_csv)
        self.start_training(
            "trader",
            "1",
            10,
            epoch_time=1626698020,
            expected_error=True,
            expected_result="not_enough_data_for_training",
        )

    def test_invalid_reward_handled_gracefully(self):
        trader_init = copy.deepcopy(self.trader_init_req)
        trader_init.actions["buy"] = "foo"

        self.init(
            trader_init,
            expected_error=True,
            expected_result="invalid_reward_function",
        )

    def test_no_rewards_handled_gracefully(self):
        trader_init = copy.deepcopy(self.trader_init_req)
        trader_init.actions.clear()

        self.init(
            trader_init,
            expected_error=True,
            expected_result="missing_actions",
        )

    def test_no_fields_handled_gracefully(self):
        trader_init = copy.deepcopy(self.trader_init_req)
        trader_init.fields.clear()

        self.init(
            trader_init,
            expected_error=True,
            expected_result="missing_fields",
        )

    def test_invalid_reward_post_error(self):
        trader_init = copy.deepcopy(self.trader_init_req)
        trader_init.actions["buy"] = "reward = foo"

        self.init(trader_init)

        self.add_data("trader", self.trader_data_csv)

        FLIGHT = "1"
        NUMBER_EPISODES = 10
        self.start_training("trader", FLIGHT, NUMBER_EPISODES, 1626697490)

        self.wait_for_training()

        self.assertEqual(len(self.episode_results), 1)
        error_data = self.episode_results[0]["episode_data"]
        self.assertEqual(error_data["error"], "invalid_reward_function")
        self.assertEqual(
            error_data["error_message"], """NameError("name 'foo' is not defined")"""
        )

    def test_unsafe_reward_error(self):
        trader_init = copy.deepcopy(self.trader_init_req)
        trader_init.actions[
            "buy"
        ] = "open('/tmp/FILE','w').write('this is unsafe!'); reward = 1"

        self.init(trader_init)

        self.add_data("trader", self.trader_data_csv)

        FLIGHT = "1"
        NUMBER_EPISODES = 10
        self.start_training("trader", FLIGHT, NUMBER_EPISODES, 1626697490)

        self.wait_for_training()

        self.assertEqual(len(self.episode_results), 1)
        error_data = self.episode_results[0]["episode_data"]
        self.assertEqual(error_data["error"], "invalid_reward_function")
        self.assertEqual(
            error_data["error_message"], """NameError("name 'open' is not defined")"""
        )

    def test_invalid_law_post_error(self):
        trader_init = copy.deepcopy(self.trader_init_req)
        trader_init.laws[0] = "can I do this?"

        self.init(trader_init)

        self.add_data("trader", self.trader_data_csv)

        FLIGHT = "1"
        NUMBER_EPISODES = 10
        self.start_training("trader", FLIGHT, NUMBER_EPISODES, 1626697490)

        self.wait_for_training()

        self.assertEqual(len(self.episode_results), 1)
        error_data = self.episode_results[0]["episode_data"]
        self.assertEqual(error_data["error"], "invalid_law_expression")
        self.assertEqual(
            error_data["error_message"],
            """SyntaxError('invalid syntax', ('<string>', 1, 5, 'can I do this?'))""",
        )

    def test_invalid_datasource_action_post_error(self):
        trader_init = copy.deepcopy(self.trader_init_req)
        trader_init.datasources[0].actions[
            "buy"
        ] = "local_portfolio_usd_balance1 -= coinbase_btcusd_close\nlocal_portfolio_btc_balance += 1"

        self.init(trader_init)

        self.add_data("trader", self.trader_data_csv)

        FLIGHT = "1"
        NUMBER_EPISODES = 10
        self.start_training("trader", FLIGHT, NUMBER_EPISODES, 1626697490)

        self.wait_for_training()

        self.assertEqual(len(self.episode_results), 1)
        error_data = self.episode_results[0]["episode_data"]
        self.assertEqual(error_data["error"], "invalid_datasource_action_expression")
        self.assertEqual(
            error_data["error_message"],
            """NameError("name 'local_portfolio_usd_balance1' is not defined")""",
        )

    def test_epoch_is_inferred_if_absent(self):
        trader_init = copy.deepcopy(self.trader_init_req)
        trader_init.epoch_time = 0
        trader_init.period = 120
        self.init(trader_init)

        now_unix_seconds = (
            pd.Timestamp.now() - pd.Timestamp("1970-01-01")
        ) // pd.Timedelta("1s")

        csv_data = io.StringIO()
        headers = [
            "time",
            "local_portfolio_usd_balance",
            "local_portfolio_btc_balance",
            "coinbase_btcusd_close",
        ]
        writer = csv.writer(csv_data)
        writer.writerow(headers)

        for unix_seconds in range(now_unix_seconds - 70, now_unix_seconds - 10, 10):
            row = [unix_seconds, None, None, 123]
            writer.writerow(row)

        self.add_data("trader", csv_data.getvalue())

        FLIGHT = "1"
        NUMBER_EPISODES = 10
        self.start_training(
            "trader",
            FLIGHT,
            NUMBER_EPISODES,
            expected_error=False,
        )

        # Counts will be unstable due to timing.  The important thing is that we launch training with enough data.
        self.wait_for_training()

    def test_add_data_with_different_fields_fails(self):
        trader_init = copy.deepcopy(self.trader_init_req)
        trader_init.epoch_time = 0
        trader_init.period = 120
        self.init(self.trader_init_req)

        now_unix_seconds = (
            pd.Timestamp.now() - pd.Timestamp("1970-01-01")
        ) // pd.Timedelta("1s")

        csv_data = io.StringIO()
        headers = [
            "time",
            "local_portfolio_usd_balance",
            "local_portfolio_btc_balance",
            "non_exist",
        ]
        writer = csv.writer(csv_data)
        writer.writerow(headers)

        for unix_seconds in range(now_unix_seconds - 70, now_unix_seconds - 10, 10):
            row = [unix_seconds, None, None, 123]
            writer.writerow(row)

        resp = self.aiengine.AddData(
            aiengine_pb2.AddDataRequest(pod="trader", csv_data=csv_data.getvalue()),
            None,
        )
        self.assertTrue(resp.error)
        self.assertEqual(resp.result, "unexpected_field")
        self.assertEqual(resp.message, "Unexpected field: 'non_exist'")


if __name__ == "__main__":
    unittest.main()
