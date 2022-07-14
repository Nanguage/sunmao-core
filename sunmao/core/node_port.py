import typing as T
from dataclasses import dataclass
from collections import deque

from .base import SunmaoObj, TypeCheckError, RangeCheckError


if T.TYPE_CHECKING:
    from .node import Node
    from .connections import Connection


class ActivateSignal(SunmaoObj):
    def __init__(self, data: T.Any = None):
        self.data = data


class NodePort(SunmaoObj):
    def __init__(self, name: str, node: "Node") -> None:
        self.name = name
        self.node = node
        self.connections: T.List["Connection"] = []


class InputPort(NodePort):
    def __init__(self, name: str, node: "Node") -> None:
        NodePort.__init__(self, name, node)
        self.signal_buffer: "deque[ActivateSignal]" = deque([])

    def put_signal(self, data=None):
        self.signal_buffer.append(ActivateSignal(data))

    def get_signal(self) -> ActivateSignal:
        return self.signal_buffer.pop()

    def __str__(self):
        return f"<InputPort {self.name} on {self.node}>"


class OutputPort(NodePort):
    def __init__(self, name: str, node: "Node") -> None:
        NodePort.__init__(self, name, node)

    def activate_successors(self):
        for conn in self.connections:
            conn.target.put_signal()
            conn.target.node.activate()


class DataPort(NodePort):
    _type_to_type_checker = {}
    _type_to_range_checker = {}

    def __init__(
            self, name: str, node: "Node",
            data_type: type, data_range: T.Optional[object]) -> None:
        NodePort.__init__(self, name, node)
        self.data_type = data_type
        self.data_range = data_range
        self._cache = None

    def set_cache(self, data: T.Any):
        self._cache = data

    def get_cache(self) -> T.Any:
        return self._cache

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
        if self.data_range is not None:
            range_checker = self._type_to_range_checker.get(self.data_type)
            if range_checker and (not range_checker(val, self.data_range)):
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

    def get_data(self) -> T.Any:
        sig = self.signal_buffer.pop()
        data = sig.data
        self.check(data)
        self.set_cache(data)
        return data


class OutputDataPort(OutputPort, DataPort):
    def __init__(
            self, name: str, node: "Node",
            data_type: type, data_range: T.Optional[object]) -> None:
        OutputPort.__init__(self, name, node)
        DataPort.__init__(self, name, node, data_type, data_range)

    def push_data(self, data: T.Any):
        self.check(data)
        self.set_cache(data)
        for conn in self.connections:
            conn.target.put_signal(data=data)


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
