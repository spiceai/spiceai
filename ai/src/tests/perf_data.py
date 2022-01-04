from io import StringIO
import time

import pandas as pd

from proto.aiengine.v1 import aiengine_pb2
import main
from tests import common


class DataSource:
    def __init__(self, name, init_json_path, data_path):
        self.name = name
        self.init_req = common.get_init_from_json(init_data_path=init_json_path, pod_name=name)
        with open(data_path, "r", encoding="utf8") as data_file:
            self.csv_data = data_file.read()


class PerfData():
    def __init__(self):
        print("Starting performance test for data\n")
        self.aiengine = main.AIEngine()

        self.data_sources = {
            "trader": DataSource(
                "trader", "../../test/assets/aiengine/api/trader_init.json", "../../test/assets/data/csv/trader.csv"),
            "coinbase": DataSource(
                "coinbase", "../../test/assets/aiengine/api/coinbase_init.json",
                "../../test/assets/data/csv/COINBASE_BTCUSD, 30.csv")
        }

    def init(self, init_req: aiengine_pb2.InitRequest, expected_error: bool = False, expected_result: str = "ok"):
        resp = self.aiengine.Init(init_req, None)
        assert resp.error == expected_error
        assert resp.result == expected_result

    def add_data(self, pod_name: str, csv_data: str):
        resp = self.aiengine.AddData(aiengine_pb2.AddDataRequest(pod=pod_name, csv_data=csv_data), None)
        assert resp.error is False

    def pandas_load(self):
        print("Pandas data loading time")
        for data_name, data_source in self.data_sources.items():
            print(f"{data_name}:", end="", flush=True)
            start_time = time.time()
            for _ in range(1000):
                pd.read_csv(StringIO(data_source.csv_data))
            end_time = time.time()

            print(f" {end_time - start_time:.02f}ms")
        print()

    def aiengine_load(self):

        accumulation = 1000
        print(f"AIEngine loading {accumulation}x data")
        for data_name, data_source in self.data_sources.items():
            print(f"{data_name}:", end="", flush=True)
            self.init(data_source.init_req)
            start_time = time.time()
            for _ in range(accumulation):
                self.add_data(data_name, data_source.csv_data)
            end_time = time.time()

            print(f" {end_time - start_time:.02f}s")
        print()


if __name__ == "__main__":
    suite = PerfData()
    suite.pandas_load()
    suite.aiengine_load()
