if __name__ == "__main__":
    import main_test_path

import io
import csv
import unittest
import main
import json
import pandas as pd
import threading
import copy


class TrainingLoopTests(unittest.TestCase):
    def setUp(self):
        self.test_client = main.app.test_client()

        with open(
            "../../test/assets/aiengine/api/trader_init.json", "r"
        ) as trader_init:
            trader_init_data = trader_init.read()
        self.trader_init = json.loads(trader_init_data)

        with open(
            "../../test/assets/aiengine/api/cartpole_init.json", "r"
        ) as cartpole_init:
            cartpole_init_data = cartpole_init.read()
        self.cartpole_init = json.loads(cartpole_init_data)

        with open("../../test/assets/data/csv/trader.csv", "r") as trader_data:
            self.trader_data_csv = trader_data.read()

        self.episode_results = list()
        self.original_post_episode_result = main.post_episode_result
        main.post_episode_result = (
            lambda request_url, episode_data: self.episode_results.append(
                {"request_url": request_url, "episode_data": episode_data}
            )
        )
        self.original_end_of_episode = main.end_of_episode

    def tearDown(self):
        main.post_episode_result = self.original_post_episode_result
        main.end_of_episode = self.original_end_of_episode

    def init(
        self,
        pod_name: str,
        init_payload: dict,
        expected_code: int = 200,
        expected_result: str = "ok",
    ):
        resp = self.test_client.post(f"/pods/{pod_name}/init", json=init_payload)
        response_json = resp.get_json()
        self.assertEqual(resp.status_code, expected_code)
        self.assertEqual(response_json["result"], expected_result)

    def post_data(self, pod_name: str, data: dict):
        resp = self.test_client.post(f"/pods/{pod_name}/data", data=data)
        self.assertEqual(resp.status_code, 200)

    def start_training(
        self,
        pod_name: str,
        flight: str = None,
        number_episodes: int = None,
        epoch_time: int = None,
        expected_code: int = 200,
        expected_result: str = "started_training",
    ):
        train_params = dict()
        if flight:
            train_params["flight"] = flight
        if number_episodes:
            train_params["number_episodes"] = number_episodes
        if epoch_time:
            train_params["epoch_time"] = epoch_time

        resp = self.test_client.post(
            f"/pods/{pod_name}/train",
            json=train_params,
        )
        response_json = resp.get_json()
        self.assertEqual(resp.status_code, expected_code)
        self.assertEqual(response_json["result"], expected_result)

    def wait_for_training(self):
        self.assertIsNotNone(main.training_thread)
        main.training_thread.join()

    def inference(self, pod_name: str, tag: str, assertion_on_response=None):
        resp = self.test_client.get(f"/pods/{pod_name}/models/{tag}/inference")
        response_json = resp.get_json()
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(response_json["tag"], tag)

        if assertion_on_response is not None:
            assertion_on_response(response_json)

    def validate_episode_data(
        self, pod_name, flight, number_episodes, num_actions, episode_results
    ):
        self.assertEqual(len(episode_results), number_episodes)
        index = episode_results[0]["episode_data"]["episode"]
        for episode_result in episode_results:
            episode_data = episode_result["episode_data"]
            self.assertEqual(
                episode_result["request_url"],
                f"http://localhost:8000/api/v0.1/pods/{pod_name}/flights/{flight}/episodes",
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
        self.init("trader", self.trader_init)

        # Step 2, load the csv data
        self.post_data("trader", self.trader_data_csv)

        FLIGHT = "1"
        NUMBER_EPISODES = 10
        # Step 3, train
        self.start_training("trader", FLIGHT, NUMBER_EPISODES)

        self.wait_for_training()

        # Step 4, inference
        self.inference(
            "trader",
            "latest",
            lambda response_json: self.assertNotEqual(response_json["confidence"], 0.0),
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
        self.init("trader", self.trader_init)

        # Step 2, load the csv data
        self.post_data("trader", self.trader_data_csv)

        FLIGHT = "1"
        NUMBER_EPISODES = 10
        # Step 3, train
        self.start_training("trader", FLIGHT, NUMBER_EPISODES, 1626697490)

        self.wait_for_training()

        # Step 4, inference
        self.inference(
            "trader",
            "latest",
            lambda response_json: self.assertNotEqual(response_json["confidence"], 0.0),
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

        self.init("trader", self.trader_init)
        self.post_data("trader", gap_data_0)
        self.post_data("trader", gap_data_1)

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
            current_time += pd.to_timedelta(self.trader_init["granularity"], unit="s")

    def test_data_added_after_training_starts(self):
        with open("./tests/assets/csv/training_loop_gap_0.csv", "r") as data:
            gap_data_0 = data.read()
        with open("./tests/assets/csv/training_loop_gap_1.csv", "r") as data:
            gap_data_1 = data.read()

        self.init("trader", self.trader_init)
        self.post_data("trader", gap_data_0)

        FLIGHT = "1"
        NUMBER_EPISODES = 10
        self.start_training("trader", FLIGHT, NUMBER_EPISODES)

        episode_5_lock = threading.Lock()
        episode_5_lock.acquire()

        def release_lock_on_episode_5(episode: int):
            if episode == 5 and episode_5_lock.locked():
                episode_5_lock.release()

        main.end_of_episode = release_lock_on_episode_5

        # wait for episode 5
        episode_5_lock.acquire()

        print("Posting gap_data_1")
        self.post_data("trader", gap_data_1)

        self.wait_for_training()
        episode_5_lock.release()

        self.validate_episode_data(
            "trader",
            FLIGHT,
            6,
            num_actions=10,
            episode_results=self.episode_results[0:6],
        )
        self.validate_episode_data(
            "trader",
            FLIGHT,
            4,
            num_actions=50,
            episode_results=self.episode_results[6:],
        )

    def test_epoch_earlier_than_data(self):
        self.init("trader", self.trader_init)
        self.post_data("trader", self.trader_data_csv)
        self.start_training(
            "trader",
            "1",
            10,
            1626697400,
            expected_code=400,
            expected_result="epoch_time_invalid",
        )

    def test_epoch_offset_from_data(self):
        self.init("trader", self.trader_init)
        self.post_data("trader", self.trader_data_csv)
        self.start_training(
            "trader",
            "1",
            1,
            1626697485,
            expected_code=200,
            expected_result="started_training",
        )
        self.wait_for_training()

    def test_epoch_after_latest_data(self):
        self.init("trader", self.trader_init)
        self.post_data("trader", self.trader_data_csv)
        self.start_training(
            "trader",
            "1",
            10,
            1626699240,
            expected_code=400,
            expected_result="not_enough_data_for_training",
        )

    def test_not_enough_data_for_training_no_data(self):
        self.init("trader", self.trader_init)
        self.start_training(
            "trader",
            "1",
            10,
            expected_code=400,
            expected_result="not_enough_data_for_training",
        )

    def test_not_enough_data_for_training_late_epoch(self):
        self.init("trader", self.trader_init)
        self.post_data("trader", self.trader_data_csv)
        self.start_training(
            "trader",
            "1",
            10,
            epoch_time=1626698020,
            expected_code=400,
            expected_result="not_enough_data_for_training",
        )

    def test_invalid_reward_handled_gracefully(self):
        trader_init = copy.deepcopy(self.trader_init)
        trader_init["actions"]["buy"] = "foo"

        self.init(
            "trader",
            trader_init,
            expected_code=400,
            expected_result="invalid_reward_function",
        )

    def test_no_rewards_handled_gracefully(self):
        trader_init = copy.deepcopy(self.trader_init)
        del trader_init["actions"]

        self.init(
            "trader",
            trader_init,
            expected_code=400,
            expected_result="missing_actions",
        )

    def test_no_fields_handled_gracefully(self):
        trader_init = copy.deepcopy(self.trader_init)
        del trader_init["fields"]

        self.init(
            "trader",
            trader_init,
            expected_code=400,
            expected_result="missing_fields",
        )

    def test_no_laws_handled_gracefully(self):
        trader_init = copy.deepcopy(self.trader_init)
        del trader_init["laws"]

        self.init(
            "trader",
            trader_init,
            expected_code=400,
            expected_result="missing_laws",
        )

    def test_invalid_reward_post_error(self):
        trader_init = copy.deepcopy(self.trader_init)
        trader_init["actions"]["buy"] = "reward = foo"

        self.init("trader", trader_init)

        self.post_data("trader", self.trader_data_csv)

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

    def test_invalid_law_post_error(self):
        trader_init = copy.deepcopy(self.trader_init)
        trader_init["laws"][0] = "can I do this?"

        self.init("trader", trader_init)

        self.post_data("trader", self.trader_data_csv)

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
        trader_init = copy.deepcopy(self.trader_init)
        trader_init["datasources"][0]["actions"][
            "buy"
        ] = "local_portfolio_usd_balance1 -= coinbase_btcusd_close\nlocal_portfolio_btc_balance += 1"

        self.init("trader", trader_init)

        self.post_data("trader", self.trader_data_csv)

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
        trader_init = copy.deepcopy(self.trader_init)
        del trader_init["epoch_time"]
        trader_init["period"] = 120
        self.init("trader", trader_init)

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

        self.post_data("trader", csv_data.getvalue())

        FLIGHT = "1"
        NUMBER_EPISODES = 10
        self.start_training(
            "trader",
            FLIGHT,
            NUMBER_EPISODES,
            expected_code=200,
        )

        # Counts will be unstable due to timing.  The important thing is that we launch training with enough data.
        self.wait_for_training()

    def test_cartpole_training(self):
        self.init("cartpole", self.cartpole_init)
        self.start_training(
            "cartpole",
            "1",
            1,
            expected_code=200,
            expected_result="started_training",
        )
        self.wait_for_training()

        # Validate the episode data
        self.validate_episode_data(
            "cartpole",
            "1",
            1,
            num_actions=299,
            episode_results=self.episode_results,
        )


if __name__ == "__main__":
    unittest.main()
