from io import StringIO
import multiprocessing.connection
import multiprocessing.queues
import threading
from typing import Dict
from connector.manager import ConnectorManager, ConnectorName

import pandas as pd
from connector.stateful import StatefulConnector

from data import DataManager, DataParam
from proto.aiengine.v1 import aiengine_pb2


class EventLoop:
    def __init__(self,
                 work_queue: multiprocessing.queues.SimpleQueue,
                 data_managers: Dict[str, DataManager],
                 connector_managers: Dict[str, ConnectorManager]):
        self.work_queue = work_queue
        self.loop_thread = None
        self.data_managers = data_managers
        self.connector_managers = connector_managers

    def start(self):
        self.loop_thread = threading.Thread(target=self._loop)
        self.loop_thread.start()

    def _loop(self):
        while True:
            event_type, params = self.work_queue.get()

            if event_type == "add_data":
                self._add_data(params)
            elif event_type == "init":
                self._init(params)

    def _add_data(self, request: aiengine_pb2.AddDataRequest):
        new_data: pd.DataFrame = pd.read_csv(StringIO(request.csv_data))
        new_data["time"] = pd.to_datetime(new_data["time"], unit="s")
        new_data = new_data.set_index("time")

        data_manager: DataManager = self.data_managers[request.pod]
        for field in new_data.columns:
            if field not in data_manager.fields.keys():
                print(f"Unexpected field: '{field}'")
                return

        data_manager = self.data_managers[request.pod]
        data_manager.merge_data(new_data)

    def _init(self, request: aiengine_pb2.InitRequest):
        connector_manager = ConnectorManager()
        action_rewards = request.actions

        period_secs = pd.to_timedelta(request.period, unit="s")
        interval_secs = pd.to_timedelta(request.interval, unit="s")
        granularity_secs = pd.to_timedelta(request.granularity, unit="s")

        epoch_time = pd.Timestamp.now() - period_secs
        if request.epoch_time != 0:
            epoch_time = pd.to_datetime(request.epoch_time, unit="s")

        data_manager = DataManager(
            param=DataParam(
                epoch_time=epoch_time,
                period_secs=period_secs,
                interval_secs=interval_secs,
                granularity_secs=granularity_secs),
            fields=request.fields,
            action_rewards=action_rewards,
            actions_order=request.actions_order,
            external_reward_funcs=request.external_reward_funcs,
            laws=request.laws,
        )
        self.data_managers[request.pod] = data_manager
        self.connector_managers[request.pod] = connector_manager

        datasources_data = request.datasources

        for datasource_data in datasources_data:
            connector_data = datasource_data.connector
            connector_name: ConnectorName = connector_data.name

            datasource_actions = datasource_data.actions

            if connector_name == ConnectorName.STATEFUL.value:
                new_connector = StatefulConnector(
                    data_manager=data_manager,
                    action_effects=datasource_actions,
                )
                connector_manager.add_connector(new_connector)
