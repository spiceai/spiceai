import io
from pathlib import Path
import socket
import threading
import unittest

import pyarrow as pa
from pyarrow import csv as arrow_csv

import main
from proto.aiengine.v1 import aiengine_pb2
from tests.common import get_init_from_json


class MainTestCase(unittest.TestCase):
    IPC_PATH = Path("/", "tmp", "spice_ai_test_main.sock")

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

    def add_data(self, pod_name: str, csv_data: str, should_error=False):
        table = arrow_csv.read_csv(io.BytesIO(csv_data.encode()))
        ready_barrier = threading.Barrier(2, timeout=2)
        ipc_thread = threading.Thread(target=self.ipc_server, args=(self.IPC_PATH, table, ready_barrier,))
        ipc_thread.start()
        ready_barrier.wait()
        resp = self.aiengine.AddData(
            aiengine_pb2.AddDataRequest(pod=pod_name, unix_socket=str(self.IPC_PATH)), None
        )
        ipc_thread.join()
        if not should_error:
            self.assertFalse(resp.error)
        return resp

    @staticmethod
    def ipc_server(ipc_path: Path, table: pa.Table, ready_barrier: threading.Barrier):
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as arrow_socket:
            if ipc_path.exists():
                ipc_path.unlink()
            try:
                arrow_socket.settimeout(2)
                arrow_socket.bind(str(ipc_path).encode())
                arrow_socket.listen(1)
                ready_barrier.wait()
                connection, _address = arrow_socket.accept()
                with connection:
                    writer = pa.ipc.RecordBatchStreamWriter(connection.makefile(mode="wb"), table.schema)
                    writer.write_table(table)
            except OSError as error:
                print(error)
            arrow_socket.shutdown(socket.SHUT_RDWR)
        ipc_path.unlink()

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
        self.add_data("trader", self.trader_data_csv)

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
        self.add_data("trader", self.trader_data_csv)

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
        self.add_data("trader", tiny_data)

        # Step 3, inference
        resp = self.aiengine.GetInference(
            aiengine_pb2.InferenceRequest(pod="trader", tag="latest"), None
        )
        self.assertTrue(resp.response.error)
        self.assertEqual(resp.response.result, "not_enough_data")


if __name__ == "__main__":
    unittest.main()
