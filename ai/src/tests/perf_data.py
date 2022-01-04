from io import StringIO
import pandas as pd
import time

from proto.aiengine.v1 import aiengine_pb2

import main
from tests import common


class PerfData():
    def __init__(self):
        print("Starting performance test for data\n")
        self.aiengine = main.AIEngine()

        self.trader_init_req = common.get_init_from_json(
            init_data_path="../../test/assets/aiengine/api/trader_init.json",
            pod_name="trader",
        )
        with open("../../test/assets/data/csv/trader.csv", "r", encoding="utf8") as trader_data:
            self.trader_data_csv = trader_data.read()

    def init(self, init_req: aiengine_pb2.InitRequest, expected_error: bool = False, expected_result: str = "ok"):
        resp = self.aiengine.Init(init_req, None)
        assert resp.error == expected_error
        assert resp.result == expected_result

    def add_data(self, pod_name: str, csv_data: str):
        resp = self.aiengine.AddData(aiengine_pb2.AddDataRequest(pod=pod_name, csv_data=csv_data), None)
        assert resp.error is False

    def pandas_load(self):
        print("Pandas data loading time:", end="")
        start_time = time.time()
        for _ in range(1000):
            pd.read_csv(StringIO(self.trader_data_csv))
        end_time = time.time()

        print(f" {end_time - start_time:.02f}ms")

    def aiengine_load(self):
        self.init(self.trader_init_req)

        accumulation = 1000
        print(f"AIEngine loading {accumulation}x data:", end="", flush=True)
        start_time = time.time()
        for _ in range(accumulation):
            self.add_data("trader", self.trader_data_csv)
        end_time = time.time()

        print(f" {end_time - start_time:.02f}s")


if __name__ == "__main__":
    suite = PerfData()
    suite.pandas_load()
    suite.aiengine_load()
