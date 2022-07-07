import typing as T
import asyncio
from dataclasses import dataclass


if T.TYPE_CHECKING:
    from .node import Node
    from .channel import Channel


class NodePort():
    def __init__(self, name: str, node: "Node") -> None:
        self.name = name
        self.node = node
        self.channels: T.List["Channel"] = []

    def connect_channel(self, channel: "Channel"):
        self.channels.append(channel)

    def disconnect_channel(self, channel: "Channel"):
        self.channels.remove(channel)

    def disconnect_channel_by_idx(self, idx: int):
        self.disconnect_channel(self.channels[idx])


class InputPort(NodePort):
    def __init__(self, name: str, node: "Node") -> None:
        NodePort.__init__(self, name, node)

    def __str__(self):
        return f"<InputPort {self.name} on {self.node}>"

    async def get_val(self) -> T.Any:
        if len(self.channels) > 0:
            val = await self.channels[0].get_val()
            return val
        else:
            raise ValueError(f"{self} not has connected channel.")

    def connect_channel(self, channel: "Channel"):
        channel.target_port = self
        if len(self.channels) > 0:
            # only keep one connected channel
            self.disconnect_channel_by_idx(0)
        return super().connect_channel(channel)

    def disconnect_channel(self, channel: "Channel"):
        channel.target_port = None
        return super().disconnect_channel(channel)


class OutputPort(NodePort):
    def __init__(self, name: str, node: "Node") -> None:
        NodePort.__init__(self, name, node)

    def connect_channel(self, channel: "Channel"):
        channel.source_port = self
        return super().connect_channel(channel)

    def disconnect_channel(self, channel: "Channel"):
        channel.source_port = None
        return super().disconnect_channel(channel)

    async def activate_successors(self):
        successor_calls = []
        for ch in self.channels:
            target_port = ch.target_port
            if target_port:
                call = target_port.node.run()
                successor_calls.append(call)
        await asyncio.gather(*successor_calls)


class CheckError(Exception):
    pass


class TypeCheckError(CheckError):
    pass


class RangeCheckError(CheckError):
    pass


class DataPort(NodePort):
    _type_to_type_checker = {}
    _type_to_range_checker = {}

    def __init__(
            self, name: str, node: "Node",
            data_type: type, data_range: T.Optional[object]) -> None:
        NodePort.__init__(self, name, node)
        self.data_type = data_type
        self.data_range = data_range

    @classmethod
    def register_type_checker(
            cls, type, func: T.Optional[T.Callable[[T.Any], bool]] = None):
        if func is None:
            func = lambda val: isinstance(val, type)  # noqa
        cls._type_to_type_checker[type] = func

    @classmethod
    def register_range_checker(
            cls, type, func: T.Callable[[T.Any, T.Any], bool]):
        cls._type_to_range_checker[type] = func

    def check(self, val):
        type_checker = self._type_to_type_checker.get(self.data_type)
        if type_checker and (not type_checker(val)):
            raise TypeCheckError(
                f"Expect type: {self.data_type}, got: {type(val)}")
        range_checker = self._type_to_range_checker.get(self.data_type)
        if range_checker and (not range_checker(self.data_range, val)):
            raise RangeCheckError(
                f"Expect range: {self.data_range}, got: {val}")


DataPort.register_type_checker(int)
DataPort.register_type_checker(float)
DataPort.register_type_checker(str)
DataPort.register_type_checker(bool)
DataPort.register_range_checker(
    int, lambda val, range_: range_[0] <= val <= range_[1])
DataPort.register_range_checker(
    float, lambda val, range_: range_[0] <= val <= range_[1])


class ExecPort(NodePort):
    def __init__(self, name: str, node: "Node") -> None:
        NodePort.__init__(self, name, node)


class InputExecPort(InputPort, ExecPort):
    def __init__(self, name: str, node: "Node") -> None:
        super().__init__(name, node)


class OutputExecPort(OutputPort, ExecPort):
    def __init__(self, name: str, node: "Node") -> None:
        super().__init__(name, node)


class InputDataPort(InputPort, DataPort):
    def __init__(
            self, name: str, node: "Node",
            data_type: type, data_range: T.Optional[object]) -> None:
        InputPort.__init__(self, name, node)
        DataPort.__init__(self, name, node, data_type, data_range)


class OutputDataPort(OutputPort, DataPort):
    def __init__(
            self, name: str, node: "Node",
            data_type: type, data_range: T.Optional[object]) -> None:
        OutputPort.__init__(self, name, node)
        DataPort.__init__(self, name, node, data_type, data_range)

    async def put_val(self, val):
        routines = []
        for ch in self.channels:
            routines.append(ch.put_val(val))
        await asyncio.gather(*routines)


@dataclass
class PortBluePrint:
    name: str
    exec: bool = False
    data_type: T.Optional[type] = None
    data_range: T.Optional[object] = None

    def to_input_port(self, node: "Node") -> InputPort:
        if self.exec:
            port = InputExecPort(self.name, node)
        else:
            port = InputDataPort(
                self.name, node, self.data_type, self.data_range)
        return port

    def to_output_port(self, node: "Node") -> OutputPort:
        if self.exec:
            port = OutputExecPort(self.name, node)
        else:
            port = OutputDataPort(
                self.name, node, self.data_type, self.data_range)
        return port
