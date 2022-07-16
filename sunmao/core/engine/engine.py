from collections import OrderedDict
import typing as T
from dataclasses import dataclass
import time

from ..base import SunmaoObj
from .job import Job


@dataclass
class EngineSetting:
    max_threads: int = 20
    max_processes: int = 8


JobStoreType = T.OrderedDict[str, Job]


class Jobs:
    def __init__(self):
        self.pending: JobStoreType = OrderedDict()
        self.running: JobStoreType = OrderedDict()
        self.done: JobStoreType = OrderedDict()
        self.failed: JobStoreType = OrderedDict()


class Engine(SunmaoObj):
    def __init__(
            self,
            setting: T.Optional[EngineSetting] = None,
            jobs: T.Optional[Jobs] = None,
            ) -> None:
        super().__init__()
        if jobs is None:
            jobs = Jobs()
        self.jobs = jobs
        if setting is None:
            setting = EngineSetting()
        self.setting = setting
        self.setup_by_setting()

    def setup_by_setting(self):
        setting = self.setting
        self.thread_counts = setting.max_threads
        self.process_count = setting.max_processes

    def submit(self, job: Job):
        assert job.status == "pending"
        job.engine = self
        self.jobs.pending[job.id] = job
        self.activate()

    def activate(self):
        for j in self.jobs.pending.values():
            if j.has_resource() and j.consume_resource():
                j.emit()
                break

    def wait(
            self, time_interval=0.01,
            print_running_jobs: bool = False):
        while True:
            if len(self.jobs.running) == 0:
                break
            if print_running_jobs:
                print(list(self.jobs.running))
            time.sleep(time_interval)
