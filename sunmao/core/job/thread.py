import typing as T

from .base import Job
from .utils import ThreadWithExc


class IThread(ThreadWithExc):
    def __init__(
            self,
            func: T.Callable, args: tuple,
            callback: T.Callable[[T.Any], None],
            error_callback: T.Callable[[Exception], None],
            ) -> None:
        super().__init__()
        self.func = func
        self.args = args
        self.callback = callback
        self.error_callback = error_callback

    def run(self):
        success = False
        try:
            res = self.func(*self.args)
            success = True
        except Exception as e:
            self.error_callback(e)
        if success:
            self.callback(res)


class ThreadJob(Job):
    def has_resource(self) -> bool:
        return self.engine.thread_counts > 0

    def consume_resource(self) -> bool:
        self.engine.thread_counts -= 1
        return True

    def release_resource(self) -> bool:
        self.engine.thread_counts += 1
        return True

    def run(self):
        self._thread = IThread(
            func=self.func, args=self.args,
            callback=self.on_done,
            error_callback=self.on_failed)
        self._thread.start()
