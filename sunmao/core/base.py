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
            sess = Session.get_current()
            flow = sess.current_flow
        self.flow = flow
        self.flow.add_obj(self)

    @property
    def session(self) -> "Session":
        return self.flow.session
