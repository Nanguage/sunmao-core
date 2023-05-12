import typing as T

from executor.engine import Engine, EngineSetting

from .base import SunmaoObj
from .flow import Flow
from .utils import logger


_current_session: T.Optional["Session"] = None


def _get_current() -> T.Optional["Session"]:
    return _current_session


def _set_current(sess: "Session"):
    global _current_session
    _current_session = sess
    logger.info(f"Current session: {sess}")


class Session(SunmaoObj):
    def __init__(
            self,
            engine_setting: T.Optional[EngineSetting] = None,
            ) -> None:
        super().__init__()
        if _get_current() is None:
            _set_current(self)
        self.flows: T.Dict[str, Flow] = {}
        self._current_flow: T.Optional[Flow] = None
        self.engine = Engine(setting=engine_setting)

    def __repr__(self) -> str:
        return f"<Session id={self.id}>"

    @property
    def current_flow(self) -> Flow:
        if self._current_flow is None:
            self._current_flow = Flow(session=self)
            logger.info(f"{self}'s current flow: {self._current_flow}")
        return self._current_flow

    @current_flow.setter
    def current_flow(self, flow: Flow):
        assert isinstance(flow, Flow)
        self._current_flow = flow
        logger.info(f"{self}'s current flow: {flow}")

    def add_flow(self, flow: Flow):
        assert isinstance(flow, Flow)
        self.flows[flow.id] = flow
        self.current_flow = flow

    @classmethod
    def get_current(cls) -> "Session":
        sess = _get_current()
        if sess is None:
            sess = cls()
        return sess

    def __enter__(self):
        self._prev_session = _current_session
        _set_current(self)
        return self

    def __exit__(self, *args):
        _set_current(self._prev_session)
        self._prev_session = None

    async def join(
            self,
            timeout: T.Optional[float] = None,
            time_delta: float = 0.01):
        await self.engine.wait_async(timeout, time_delta)
