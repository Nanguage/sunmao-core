import typing as T
from .base import SunmaoObj, FlowElement
from .node import Node
from .connection import Connection
from .node_port import (
    InputPort, OutputPort, InputDataPort, OutputDataPort
)

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

    @property
    def free_input_ports(self) -> T.List["InputPort"]:
        ports = []
        for node in self.nodes.values():
            ports.extend(node.free_input_ports)
        return ports

    @property
    def free_output_ports(self) -> T.List["OutputPort"]:
        ports = []
        for node in self.nodes.values():
            ports.extend(node.free_output_ports)
        return ports

    def __enter__(self):
        self._prev_flow = self.session.current_flow
        self.session.current_flow = self
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.session.current_flow = self._prev_flow

    async def __call__(self, inputs: dict) -> dict:
        """Intreface for execute the flow."""
        free_input_nodes: T.Set[Node] = set()
        for in_port in self.free_input_ports:
            if isinstance(in_port, InputDataPort):
                node_name = in_port.node.name
                if in_port.name in inputs:
                    data = inputs[in_port.name]
                elif f"{node_name}.{in_port.name}" in inputs:
                    data = inputs[f"{node_name}.{in_port.name}"]
                else:
                    raise ValueError(
                        f"Input port {in_port} is not provided."
                    )
                in_port.put_signal(data=data)
            else:
                in_port.put_signal()
            free_input_nodes.add(in_port.node)
        for node in free_input_nodes:
            await node.activate()
        await self.session.join()
        res = {}
        for out_port in self.free_output_ports:
            key = f"{out_port.node.name}.{out_port.name}"
            if isinstance(out_port, OutputDataPort):
                res[key] = out_port.cache
        return res
