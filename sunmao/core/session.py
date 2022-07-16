import typing as T
from .base import SunmaoObj

from .flow import Flow
from .engine.engine import Engine, EngineSetting


_current_session: T.Optional["Session"] = None


def get_current_session():
    global _current_session
    if _current_session is None:
        _current_session = Session()
    return _current_session


class Session(SunmaoObj):
    def __init__(
            self,
            engine_setting: T.Optional[EngineSetting] = None,
            ) -> None:
        super().__init__()
        global _current_session
        _current_session = self
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
