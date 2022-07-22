import typing as T
from dataclasses import dataclass
from collections import deque

from .base import FlowElement
from .error import TypeCheckError, RangeCheckError
from .connection import Connection


if T.TYPE_CHECKING:
    from .node import Node


class ActivateSignal(FlowElement):
    def __init__(
            self, data: T.Any = None,
            **kwargs):
        super().__init__(**kwargs)
        self.data = data


class NodePort(FlowElement):
    def __init__(
            self, name: str, node: "Node",
            **kwargs) -> None:
        super().__init__(**kwargs)
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

    def clear_signal_buffer(self):
        while len(self.signal_buffer) > 0:
            self.get_signal()

    def __str__(self):
        return f"<InputPort {self.name} on {self.node}>"

    @property
    def predecessor(self) -> T.Optional["OutputPort"]:
        if len(self.connections) == 0:
            return None
        else:
            conn = self.connections[0]
            return conn.source


class OutputPort(NodePort):
    def __init__(self, name: str, node: "Node") -> None:
        NodePort.__init__(self, name, node)

    def activate_successors(self):
        for s in self.successors:
            s.put_signal()
            s.node.activate()

    def connect_with(self, other: InputPort):
        conn = Connection(self, other)
        if not (conn in self.connections):
            self.connections.append(conn)
        # keep only one connection in InputPort
        if len(other.connections) == 0:
            other.connections.append(conn)
        else:
            other.connections[0] = conn

    def disconnect(self, other: InputPort):
        conn = Connection(self, other)
        if conn in self.connections:
            self.connections.remove(conn)
        if conn in other.connections:
            other.connections.remove(conn)

    @property
    def successors(self) -> T.List["InputPort"]:
        return [conn.target for conn in self.connections]


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
            data_type: type, data_range: T.Optional[object],
            data_default: T.Optional[T.Any] = None) -> None:
        InputPort.__init__(self, name, node)
        DataPort.__init__(self, name, node, data_type, data_range)
        self.default = data_default

    def set_default(self, val: T.Optional[T.Any]):
        if val is not None:
            self.check(val)
        self._default = val

    def get_default(self) -> T.Any:
        return self._default

    default = property(
        fget=get_default,
        fset=set_default,
    )

    def get_data(self) -> T.Any:
        sig = self.signal_buffer.pop()
        data = sig.data
        self.check(data)
        return data

    def fetch_missing(self) -> T.Optional[T.Any]:
        """Try to get data with:
        1. predecessor's cache
        2. default value
        """
        pre = self.predecessor
        if (pre is not None) and isinstance(pre, OutputDataPort):
            return pre.cache
        return self.default


class OutputDataPort(OutputPort, DataPort):
    def __init__(
            self, name: str, node: "Node",
            data_type: type, data_range: T.Optional[object]) -> None:
        OutputPort.__init__(self, name, node)
        DataPort.__init__(self, name, node, data_type, data_range)
        self._cache: T.Optional[T.Any] = None

    def push_data(self, data: T.Any):
        self.check(data)
        self.set_cache(data)
        for conn in self.connections:
            conn.target.put_signal(data=data)
            conn.target.node.activate()

    def set_cache(self, data: T.Any):
        self.check(data)
        self._cache = data

    def get_cache(self) -> T.Any:
        return self._cache

    def clear_cache(self):
        self._cache = None

    cache = property(
        fget=get_cache,
        fset=set_cache,
    )


@dataclass
class PortBluePrint:
    name: str
    exec: bool = False
    data_type: T.Optional[type] = None
    data_range: T.Optional[object] = None
    data_default: T.Optional[object] = None

    def to_input_port(self, node: "Node") -> InputPort:
        if self.exec:
            port = InputExecPort(self.name, node)
        else:
            port = InputDataPort(
                self.name, node, self.data_type, self.data_range,
                self.data_default)
        return port

    def to_output_port(self, node: "Node") -> OutputPort:
        if self.exec:
            port = OutputExecPort(self.name, node)
        else:
            port = OutputDataPort(
                self.name, node, self.data_type, self.data_range)
        return port
