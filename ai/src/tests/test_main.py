import unittest
import main
import json


class MainTestCase(unittest.TestCase):
    def setUp(self):
        self.test_client = main.app.test_client()

        with open(
            "../../test/assets/aiengine/api/trader_init.json", "r"
        ) as trader_init:
            trader_init_data = trader_init.read()
        self.trader_init = json.loads(trader_init_data)

        with open("../../test/assets/data/csv/trader.csv", "r") as trader_data:
            self.trader_data_csv = trader_data.read()

    def test_inference_not_initialized(self):
        resp = self.test_client.get("/pods/trader/models/latest/inference")
        response_json = resp.get_json()
        self.assertEqual(resp.status_code, 404)
        self.assertEqual(response_json["result"], "pod_not_initialized")

    def test_inference_trader_not_trained(self):
        # Step 1, init the pod
        resp = self.test_client.post("/pods/trader/init", json=self.trader_init)
        response_json = resp.get_json()
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(response_json["result"], "ok")

        # Step 2, load the csv data
        resp = self.test_client.post("/pods/trader/data", data=self.trader_data_csv)
        response_json = resp.get_json()
        self.assertEqual(resp.status_code, 200)

        # Step 3, inference
        resp = self.test_client.get("/pods/trader/models/latest/inference")
        response_json = resp.get_json()
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(response_json["confidence"], 0.0)
        self.assertEqual(response_json["tag"], "latest")


if __name__ == "__main__":
    unittest.main()
