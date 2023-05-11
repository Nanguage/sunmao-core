import typing as T
from .base import SunmaoObj, FlowElement
from .node import Node
from .connection import Connection

if T.TYPE_CHECKING:
    from .session import Session


class Flow(SunmaoObj):
    def __init__(
            self,
            name: T.Optional[str] = None,
            session: T.Optional["Session"] = None,
            ) -> None:
        super().__init__()
        if name is None:
            name = "flow_" + self.id[-8:]
        self.name = name
        self._obj_ids: set = set()
        self.nodes: T.Dict[str, Node] = {}
        self.connections: T.Dict[str, Connection] = {}
        self.other_objs: T.Dict[str, FlowElement] = {}
        if session is None:
            from .session import Session
            session = Session.get_current()
        self.session = session
        self.session.add_flow(self)

    def add_obj(self, obj: FlowElement):
        if obj in self:
            return
        if isinstance(obj, Node):
            self.nodes[obj.id] = obj
        elif isinstance(obj, Connection):
            self.connections[obj.id] = obj
        else:
            assert isinstance(obj, FlowElement)
            self.other_objs[obj.id] = obj
        self._obj_ids.add(obj.id)

    def __contains__(self, obj: FlowElement) -> bool:
        return (obj.id in self._obj_ids)

    def remove_obj(self, obj: FlowElement):
        if not (obj in self):
            return
        if isinstance(obj, Node):
            self.nodes.pop(obj.id)
        elif isinstance(obj, Connection):
            self.connections.pop(obj.id)
        else:
            assert isinstance(obj, FlowElement)
            self.other_objs.pop(obj.id)
        self._obj_ids.remove(obj.id)
