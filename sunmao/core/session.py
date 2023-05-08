import typing as T
from .base import SunmaoObj

from .flow import Flow
from executor.engine import Engine, EngineSetting


_current_session: T.Optional["Session"] = None


def get_current_session() -> T.Optional["Session"]:
    return _current_session


def set_current_session(sess: "Session"):
    global _current_session
    _current_session = sess


class Session(SunmaoObj):
    def __init__(
            self,
            engine_setting: T.Optional[EngineSetting] = None,
            ) -> None:
        super().__init__()
        if get_current_session() is None:
            set_current_session(self)
        self.flows: T.Dict[str, Flow] = {}
        self._current_flow: T.Optional[Flow] = None
        self.engine = Engine(setting=engine_setting)

    @property
    def current_flow(self) -> Flow:
        if self._current_flow is None:
            self._current_flow = Flow(session=self)
        return self._current_flow

    def add_flow(self, flow: Flow):
        assert isinstance(flow, Flow)
        self.flows[flow.id] = flow
        self._current_flow = flow

    @classmethod
    def get_current(cls) -> "Session":
        sess = get_current_session()
        if sess is None:
            sess = cls()
        return sess

    def __enter__(self):
        self.engine.start()
        self._prev_session = _current_session
        return self

    def __exit__(self, *args):
        self.engine.stop()
        set_current_session(self._prev_session)
        self._prev_session = None
