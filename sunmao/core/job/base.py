import typing as T
from ..base import SunmaoObj
from ..utils import CheckAttrRange, CheckAttrType
from ..error import SunmaoError


if T.TYPE_CHECKING:
    from ..engine import Engine


class JobStatus(CheckAttrRange):
    valid_range = ('pending', 'running', 'failed', 'done', 'canceled')
    attr = "_status"


def check_engine(obj) -> bool:
    from ..engine import Engine
    return isinstance(obj, Engine)


class EngineAttr(CheckAttrType):
    valid_type = (type(None), check_engine)


class JobEmitError(SunmaoError):
    pass


class Job(SunmaoObj):

    status = JobStatus()
    engine = EngineAttr()

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
        self.status: str = "pending"
        self.engine: T.Optional["Engine"] = None

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
        getattr(self.engine.jobs, self.status).pop(self.id)
        self.engine.jobs.running[self.id] = self
        self.status = "running"
        self.run()

    def on_done(self, res):
        self.callback(res)
        self.status = "done"
        self.engine.jobs.running.pop(self.id)
        self.engine.jobs.done[self.id] = self
        self.release_resource()
        self.engine.activate()

    def on_failed(self, e: Exception):
        self.status = "failed"
        self.error_callback(e)
        self.engine.jobs.running.pop(self.id)
        self.engine.jobs.failed[self.id] = self
        self.release_resource()
        self.engine.activate()

    def run(self):
        pass

    def cancel(self):
        if self.status != "running":
            return
        try:
            self.cancel_task()
            self.engine.jobs.running.pop(self.id)
            self.engine.jobs.cannceled[self.id] = self
            self.status = "canceled"
        except Exception:
            pass

    def cancel_task(self):
        pass


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
