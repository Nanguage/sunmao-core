import typing as T
from queue import Queue

from ..base import SunmaoObj
from ..utils import CheckAttrRange
from ..error import SunmaoError


if T.TYPE_CHECKING:
    from ..engine import Engine
    from ..node import ComputeNode


JobStatusType = T.Literal['pending', 'running', 'failed', 'done', 'canceled']
valid_job_statuses = JobStatusType.__args__  # type: ignore


class JobStatusAttr(CheckAttrRange):
    valid_range: T.Iterable[JobStatusType] = valid_job_statuses
    attr = "_status"

    def __set__(self, obj: "RawJob", value: JobStatusType):
        self.check(obj, value)
        if obj.engine is not None:
            obj.engine.jobs.move_job_store(obj, value)
        setattr(obj, self.attr, value)


class JobEmitError(SunmaoError):
    pass


class RawJob(SunmaoObj):

    status = JobStatusAttr()

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
        self.engine: T.Optional["Engine"] = None
        self._status: str = "pending"
        self._for_join: Queue = Queue()

    def __repr__(self) -> str:
        return f"<Job status={self.status} id={self.id[-8:]} func={self.func}>"

    def has_resource(self) -> bool:
        return True

    def consume_resource(self) -> bool:
        return True

    def release_resource(self) -> bool:
        return True

    def emit(self):
        _valid_status = ("pending", "canceled", "done", "failed")
        if self.status not in _valid_status:
            raise JobEmitError(
                f"{self} is not in valid status({_valid_status})")
        self.status = "running"
        self._for_join.put(0)
        self.run()

    def _on_finish(self, new_status: JobStatusType = "done"):
        self.status = new_status
        self.release_resource()
        if self.engine is not None:
            self.engine.activate()
        self._for_join.task_done()
        self._for_join.get()

    def on_done(self, res):
        self.callback(res)
        self._on_finish("done")

    def on_failed(self, e: Exception):
        self.error_callback(e)
        self._on_finish("failed")

    def join(self):
        self._for_join.join()

    def run(self):
        pass

    def cancel(self):
        if self.status != "running":
            return
        try:
            self.cancel_task()
        except Exception as e:
            print(str(e))
        finally:
            self._on_finish("canceled")

    def cancel_task(self):
        pass


class Job(RawJob):
    def __init__(self, node: "ComputeNode", args: tuple) -> None:
        self.node = node
        super().__init__(node.func, args, node.callback, node.error_callback)

    def __repr__(self) -> str:
        return f"<Job status={self.status} id={self.id[-8:]} node={self.node}>"

    @property
    def result(self) -> T.Optional[T.Any]:
        if self.status == "done":
            return self.node.caches
        else:
            return None


class LocalJob(Job):
    def run(self):
        success = False
        try:
            res = self.func(*self.args)
            success = True
        except Exception as e:
            self.on_failed(e)
        if success:
            self.on_done(res)
