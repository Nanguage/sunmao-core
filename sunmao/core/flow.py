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
        self._prev_flow = self.session._env_flow
        self.session.current_flow = self
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.session.current_flow = self._prev_flow

    def copy(self) -> "Flow":
        """Copy the flow."""
        old_id2new_node: T.Dict[str, "Node"] = {}
        flow = Flow()
        for node in list(self.nodes.values()):
            new_node = node.copy()
            new_node.flow = flow
            old_id2new_node[node.id] = new_node
            flow.add_obj(new_node)
        for conn in list(self.connections.values()):
            old_src_node_id = conn.source.node.id
            old_src_index = conn.source.index
            new_src_node = old_id2new_node[old_src_node_id]
            new_src_port = new_src_node.output_ports[old_src_index]
            old_dst_node_id = conn.target.node.id
            old_dst_index = conn.target.index
            new_dst_node = old_id2new_node[old_dst_node_id]
            new_dst_port = new_dst_node.input_ports[old_dst_index]
            new_src_port.connect_with(new_dst_port)
        return flow

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
