import math
import datetime
import os

import humanize

from utils import print_event
from metrics import metrics

PROGRESS_STEPS = 1000


class ProgressBar:
    def __init__(self, pod_name, episode_num, total_steps):
        self.pod_name = pod_name
        self.episode_num = episode_num
        self.total_steps = total_steps
        self.step = 0
        self.timer = datetime.datetime.now()
        self.show_metrics = os.getenv("SPICE_DEBUG") == "1"

    def next(self):
        self.step += 1
        if self.step % PROGRESS_STEPS != 0:
            return

        remaining_steps = math.floor((self.total_steps - self.step) / PROGRESS_STEPS)
        delta = self.timer - datetime.datetime.now()
        est_remaining_time = humanize.naturaldelta(delta * remaining_steps)
        print_event(
            self.pod_name,
            f"\tEpisode {self.episode_num}: Processed {self.step}/{self.total_steps} data points...",
        )
        print_event(
            self.pod_name, f"\t\tEstimated time remaining: {est_remaining_time}"
        )

        if self.show_metrics:
            print_event(self.pod_name, "")
            print_event(self.pod_name, "\tDebug Metrics")
            for metric_name in metrics.get_all_metric_names():
                total_seconds = metrics.get_metric(metric_name).total_seconds()
                metric_value = humanize.precisedelta(
                    datetime.timedelta(seconds=total_seconds)
                )
                print_event(self.pod_name, f"\t\t{metric_name}:\t{metric_value}")
        self.timer = datetime.datetime.now()
