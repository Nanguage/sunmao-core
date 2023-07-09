import typing as T
import uuid


if T.TYPE_CHECKING:
    from .flow import Flow
    from .session import Session


class SunmaoObj(object):
    def __init__(self):
        self.id = str(uuid.uuid4())


class FlowElement(SunmaoObj):
    def __init__(self, flow: T.Optional["Flow"] = None):
        super().__init__()
        if flow is None:
            from .session import Session
            flow = Session.get_current().current_flow
        self._flow: T.Optional["Flow"] = None
        self.flow = flow

    @property
    def flow(self) -> T.Optional["Flow"]:
        return self._flow

    @flow.setter
    def flow(self, flow: T.Optional["Flow"]):
        if self._flow is not None:
            self._flow.remove_obj(self)
        self._flow = flow
        if self._flow is not None:
            self._flow.add_obj(self)

    @property
    def session(self) -> "Session":
        if self.flow is None:
            raise RuntimeError("No flow is associated with this object")
        return self.flow.session
