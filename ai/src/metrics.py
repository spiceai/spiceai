import pandas as pd


class Metrics:
    def __init__(self):
        self.reset()

    def reset(self):
        self.metrics = {}
        self.metrics_timer = {}

    def get_metric(self, metric_name: str) -> pd.Timedelta:
        return self.metrics[metric_name]

    def get_all_metric_names(self) -> "list[str]":
        return list(self.metrics.keys())

    def start(self, metric_name: str):
        if metric_name not in self.metrics:
            self.metrics[metric_name] = pd.to_timedelta(0, unit="ms")

        self.metrics_timer[metric_name] = pd.Timestamp.now()

    def end(self, metric_name):
        if metric_name not in self.metrics_timer:
            print(f"ERROR: measure_metric_end: {metric_name} not started!")
            return

        delta = pd.Timestamp.now() - self.metrics_timer[metric_name]
        self.metrics[metric_name] += delta
        del self.metrics_timer[metric_name]


metrics = Metrics()
