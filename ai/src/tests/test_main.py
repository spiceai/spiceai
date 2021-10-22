import unittest

import main
from proto.aiengine.v1 import aiengine_pb2
from tests.common import get_init_from_json


class MainTestCase(unittest.TestCase):
    def setUp(self):
        self.aiengine = main.AIEngine()

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
