from io import StringIO
import multiprocessing
import queue
import threading
from typing import Dict

import pandas as pd

from connector.manager import ConnectorManager
from data_manager.base_manager import DataManagerBase
from dispatcher import locks
from proto.aiengine.v1.aiengine_pb2 import AddDataRequest, Response
from train import Trainer


class DataDispatcher:
    def __init__(self,
                 work_queue: multiprocessing.Queue,
                 data_managers: Dict[str, DataManagerBase],
                 connector_managers: Dict[str, ConnectorManager]):
        self.loop_thread = None
        self.work_queue = work_queue
        self.data_managers = data_managers
        self.connector_managers = connector_managers
        self.stop_work = False
        self._test_hook_callback = {}

    def start(self):
        if self.loop_thread is not None:
            return

        self.loop_thread = threading.Thread(target=self._loop)
        self.loop_thread.start()

    def stop(self):
        if self.loop_thread is None:
            return
        self.stop_work = True
        self.loop_thread.join()

    def _loop(self):
        while True:
            try:
                event_type, params = self.work_queue.get(block=True, timeout=0.1)
            except queue.Empty:
                if self.stop_work:
                    return
                continue

            if event_type == "add_data":
                resp = self._add_data(params)
                if "add_data" in self._test_hook_callback:
                    self._test_hook_callback["add_data"](resp)

    def _add_data(self, request: AddDataRequest):
        new_data: pd.DataFrame = pd.read_csv(StringIO(request.csv_data))
        new_data["time"] = pd.to_datetime(new_data["time"], unit="s")
        new_data = new_data.set_index("time")

        with locks.INIT_LOCK:
            data_manager: DataManagerBase = self.data_managers[request.pod]
            for field in new_data.columns:
                if field not in data_manager.fields.keys():
                    message = f"Unexpected field: '{field}'"
                    return Response(result="unexpected_field", message=message, error=True)

            if data_manager.dataspace_hash != request.dataspace_hash:
                message = "Dataspace hash doesn't match current data_manager"
                return Response(result="dataspace_hash_invalid", message=message, error=True)

            with Trainer.TRAINING_LOCK:
                data_manager.merge_data(new_data)

        return Response(result="ok", error=False)
