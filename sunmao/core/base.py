import typing as T
import uuid


if T.TYPE_CHECKING:
    from .flow import Flow
    from .session import Session


class SunmaoObj(object):
    def __new__(cls: "SunmaoObj", *args, **kwargs) -> "SunmaoObj":
        obj = super().__new__(cls)
        obj.id = str(uuid.uuid4())
        return obj


class FlowElement(SunmaoObj):
    def __init__(self, flow: T.Optional["Flow"] = None):
        if flow is None:
            from .session import get_current_session
            sess = get_current_session()
            flow = sess.current_flow
        self.flow = flow
        self.flow.add_obj(self)

    @property
    def session(self) -> "Session":
        return self.flow.session
