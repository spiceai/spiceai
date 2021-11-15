from io import StringIO
import multiprocessing
import threading
from typing import Dict

import pandas as pd

from connector.manager import ConnectorManager
from dispatcher import locks
from data import DataManager
from proto.aiengine.v1.aiengine_pb2 import AddDataRequest
from train import Trainer


class DataDispatcher:
    def __init__(self,
                 work_queue: multiprocessing.SimpleQueue,
                 data_managers: Dict[str, DataManager],
                 connector_managers: Dict[str, ConnectorManager]):
        self.loop_thread = None
        self.work_queue = work_queue
        self.data_managers = data_managers
        self.connector_managers = connector_managers

    def start(self):
        if self.loop_thread is not None:
            return

        self.loop_thread = threading.Thread(target=self._loop)
        self.loop_thread.start()

    def _loop(self):
        while True:
            event_type, params = self.work_queue.get()

            if event_type == "add_data":
                self._add_data(params)

    def _add_data(self, request: AddDataRequest):
        new_data: pd.DataFrame = pd.read_csv(StringIO(request.csv_data))
        new_data["time"] = pd.to_datetime(new_data["time"], unit="s")
        new_data = new_data.set_index("time")

        with locks.INIT_LOCK:
            data_manager: DataManager = self.data_managers[request.pod]
            for field in new_data.columns:
                if field not in data_manager.fields.keys():
                    print(f"Unexpected field: '{field}'")
                    return

            if data_manager.dataspace_hash != request.dataspace_hash:
                print("Dataspace hash doesn't match current data_manager")
                return

            with Trainer.TRAINING_LOCK:
                data_manager.merge_data(new_data)
