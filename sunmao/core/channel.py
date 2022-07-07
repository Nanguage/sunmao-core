import typing as T
import asyncio


if T.TYPE_CHECKING:
    from .node import Node
    from .node_port import NodePort, InputPort, OutputPort


class SourcePort:
    def __get__(self, obj: "Channel", objtype) -> T.Optional["OutputPort"]:
        return obj._source

    def __set__(self, obj: "Channel", value: T.Optional["OutputPort"]):
        from .node_port import OutputPort
        assert (value is None) or isinstance(value, OutputPort)
        obj._source = value


class TargetPort:
    def __get__(self, obj: "Channel", objtype) -> T.Optional["InputPort"]:
        return obj._target

    def __set__(self, obj: "Channel", value: T.Optional["InputPort"]):
        from .node_port import InputPort
        assert (value is None) or isinstance(value, InputPort)
        obj._target = value


class Channel(object):
    source_port = SourcePort()
    target_port = TargetPort()

    def __init__(self) -> None:
        self.queue = asyncio.Queue()
        self.source_port = None
        self.target_port = None

    async def get_val(self) -> T.Any:
        return await self.queue.get()

    async def put_val(self, val: T.Any):
        await self.queue.put(val)

    def connect_with_node(self, node: "Node", port_idx: int):
        port = node.input_ports[port_idx]
        self.connect_with_port(port)

    def connect_with_port(self, port: "NodePort"):
        port.connect_channel(self)
