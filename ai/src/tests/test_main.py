import unittest

import main
from proto.aiengine.v1 import aiengine_pb2
from tests.common import get_init_from_json


class MainTestCase(unittest.TestCase):
    def setUp(self):
        self.aiengine = main.AIEngine()

        main.data_managers.clear()
        main.connector_managers.clear()

        self.trader_init_req = get_init_from_json(
            init_data_path="../../test/assets/aiengine/api/trader_init.json",
            pod_name="trader",
        )

        with open("../../test/assets/data/csv/trader.csv", "r", encoding="utf8") as trader_data:
            self.trader_data_csv = trader_data.read()

    def test_inference_not_initialized(self):
        req = aiengine_pb2.InferenceRequest(pod="trader", tag="latest")
        resp = self.aiengine.GetInference(req, None)
        response = resp.response
        self.assertTrue(response.error)
        self.assertEqual(response.result, "pod_not_initialized")

    def test_inference_trader_not_trained(self):
        # Step 1, init the pod
        resp = self.aiengine.Init(self.trader_init_req, None)
        self.assertFalse(resp.error)
        self.assertEqual(resp.result, "ok")

        # Step 2, load the csv data
        resp = self.aiengine.AddData(
            aiengine_pb2.AddDataRequest(pod="trader", csv_data=self.trader_data_csv),
            None,
        )
        self.assertFalse(resp.error)

        # Step 3, inference
        resp = self.aiengine.GetInference(
            aiengine_pb2.InferenceRequest(pod="trader", tag="latest"), None
        )
        self.assertFalse(resp.response.error)
        self.assertEqual(resp.confidence, 0.0)
        self.assertEqual(resp.tag, "latest")
        self.assertEqual(resp.start, 1626697980)
        self.assertEqual(resp.end, 1626698040)

    def load_trader_with_data(self):
        # Step 1, init the pod
        resp = self.aiengine.Init(self.trader_init_req, None)
        self.assertFalse(resp.error)
        self.assertEqual(resp.result, "ok")

        # Step 2, load the csv data
        resp = self.aiengine.AddData(
            aiengine_pb2.AddDataRequest(pod="trader", csv_data=self.trader_data_csv),
            None,
        )
        self.assertFalse(resp.error)

    def inference_time_test(self, inference_time, should_error):
        self.load_trader_with_data()

        # Step 3, inference
        resp = self.aiengine.GetInference(
            aiengine_pb2.InferenceRequest(pod="trader", inference_time=inference_time, tag="latest"), None
        )
        if should_error:
            self.assertTrue(resp.response.error)
            self.assertEqual(resp.response.result, "invalid_recommendation_time")
            expected_message = f"The time specified ({inference_time}) is outside of the allowed range: "\
                "(1626697540, 1626698040)"
            self.assertEqual(resp.response.message, expected_message)
        else:
            self.assertFalse(resp.response.error)
            self.assertEqual(resp.end, inference_time)
            self.assertEqual(resp.start, inference_time - 60)

    def test_inference_inference_time_too_early(self):
        self.load_trader_with_data()
        self.inference_time_test(1626697480, should_error=True)

    def test_inference_inference_time_too_late(self):
        self.load_trader_with_data()
        self.inference_time_test(1626698041, should_error=True)

    def test_inference_inference_time_earliest_valid_time(self):
        self.load_trader_with_data()
        self.inference_time_test(1626697540, should_error=False)

    def test_inference_inference_time_latest_valid_time(self):
        self.load_trader_with_data()
        self.inference_time_test(1626698040, should_error=False)

    def test_inference_inference_time_middle_time(self):
        self.load_trader_with_data()
        self.inference_time_test(1626697740, should_error=False)

    def test_inference_trader_not_trained_no_data(self):
        # Step 1, init the pod
        resp = self.aiengine.Init(self.trader_init_req, None)
        self.assertFalse(resp.error)
        self.assertEqual(resp.result, "ok")

        # Step 2, inference
        resp = self.aiengine.GetInference(
            aiengine_pb2.InferenceRequest(pod="trader", tag="latest"), None
        )
        self.assertTrue(resp.response.error)
        self.assertEqual(resp.response.result, "not_enough_data")

    def test_inference_trader_not_trained_not_enough_data(self):
        # Step 1, init the pod
        resp = self.aiengine.Init(self.trader_init_req, None)
        self.assertFalse(resp.error)
        self.assertEqual(resp.result, "ok")

        # Step 2, load too little data
        tiny_data = "\n".join(self.trader_data_csv.splitlines()[0:3])
        resp = self.aiengine.AddData(
            aiengine_pb2.AddDataRequest(pod="trader", csv_data=tiny_data), None
        )
        self.assertFalse(resp.error)

        # Step 3, inference
        resp = self.aiengine.GetInference(
            aiengine_pb2.InferenceRequest(pod="trader", tag="latest"), None
        )
        self.assertTrue(resp.response.error)
        self.assertEqual(resp.response.result, "not_enough_data")


if __name__ == "__main__":
    unittest.main()
