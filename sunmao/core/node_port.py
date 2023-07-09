import typing as T
from datetime import datetime
from collections import deque
from funcdesc.desc import Value

from .connection import Connection


if T.TYPE_CHECKING:
    from .node import Node


class ActivateSignal():
    def __init__(self, data: T.Any = None):
        self.data = data


class NodePort():
    def __init__(self, name: str, node: "Node") -> None:
        self.name = name
        self.node = node
        self.connections: T.Set["Connection"] = set()


class InputPort(NodePort):
    def __init__(self, name: str, node: "Node") -> None:
        NodePort.__init__(self, name, node)
        self.signal_buffer: T.Deque[ActivateSignal] = deque([])
        self.lastest_signal_provider: T.Optional[OutputPort] = None

    @property
    def index(self) -> int:
        return self.node.input_ports.index(self)

    def put_signal(
            self, provider: T.Optional["OutputPort"] = None,
            data=None):
        self.signal_buffer.append(ActivateSignal(data))
        self.lastest_signal_provider = provider

    def get_signal(self) -> ActivateSignal:
        return self.signal_buffer.pop()

    def clear_signal_buffer(self):
        while len(self.signal_buffer) > 0:
            self.get_signal()

    def __str__(self):
        return f"<InputPort {self.name} on {self.node}>"

    @property
    def predecessors(self) -> T.Set["OutputPort"]:
        return {conn.source for conn in self.connections}


class OutputPort(NodePort):
    def __init__(self, name: str, node: "Node") -> None:
        NodePort.__init__(self, name, node)
        self.callbacks: T.List[T.Callable[[T.Any], None]] = []

    @property
    def index(self) -> int:
        return self.node.output_ports.index(self)

    def register_callback(self, func: T.Callable[[T.Any], None]):
        self.callbacks.append(func)

    async def push_signal(self, data=None):
        for callback in self.callbacks:
            callback(data)
        for s in self.successors:
            s.put_signal(provider=self, data=data)
            await s.node.activate()

    def connect_with(self, other: InputPort):
        assert self.node.flow is other.node.flow
        conn = Connection(self, other, flow=self.node.flow)
        self.connections.add(conn)
        other.connections.add(conn)

    def disconnect(self, other: InputPort):
        conn = Connection(self, other)
        if conn in self.connections:
            self.connections.remove(conn)
        if conn in other.connections:
            other.connections.remove(conn)

    @property
    def successors(self) -> T.Set["InputPort"]:
        return {conn.target for conn in self.connections}


class DataPort(NodePort):
    def __init__(
            self, name: str, node: "Node",
            val_desc: T.Optional["Value"] = None) -> None:
        NodePort.__init__(self, name, node)
        if val_desc is not None:
            self.val_desc = val_desc
        else:
            self.val_desc = Value(name=name)

    def check(self, val):
        self.val_desc.check_range(val)
        self.val_desc.check_type(val)


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
            val_desc: T.Optional[Value] = None) -> None:
        InputPort.__init__(self, name, node)
        DataPort.__init__(self, name, node, val_desc)

    def get_data(self) -> T.Any:
        sig = self.signal_buffer.pop()
        data = sig.data
        self.check(data)
        return data

    def fetch_missing(self) -> T.Optional[T.Any]:
        """Try to get data with:
        1. lastest signal provider's cache
        2. default value
        """
        pre = self.lastest_signal_provider
        if (pre is not None) and isinstance(pre, OutputDataPort):
            return pre.cache
        return self.val_desc.default


class OutputDataPort(OutputPort, DataPort):
    def __init__(
            self, name: str, node: "Node", save_cache: bool = True,
            val_desc: T.Optional[Value] = None) -> None:
        OutputPort.__init__(self, name, node)
        DataPort.__init__(self, name, node, val_desc)
        self.save_cache = save_cache
        self.last_cache_time: T.Optional[datetime] = None
        self._cache: T.Optional[T.Any] = None

    async def push_signal(self, data=None):
        self.check(data)
        if self.save_cache:
            self.set_cache(data)
        await super().push_signal(data)

    def set_cache(self, data: T.Any):
        self.check(data)
        self.last_cache_time = datetime.now()
        self._cache = data

    def get_cache(self) -> T.Any:
        return self._cache

    def clear_cache(self):
        self._cache = None

    cache = property(
        fget=get_cache,
        fset=set_cache,
    )


class Port:
    """The blueprint of a port.

    Args:
        name (str): The name of the port.
        exec (bool, optional): Whether the port is an exec port.
            Defaults to False.
        type (T.Optional[type], optional): The type of the port.
            Defaults to None.
        range (T.Optional[object], optional): The range of the port.
            Defaults to None.
        default (T.Optional[object], optional): The default value of the port.
            Defaults to None.
        save_cache (bool, optional): Whether the port should save cache.
            Defaults to True. Only available for output ports.
        **kwargs: Other attributes of the port. Used for create a Value object.
    """
    def __init__(
            self, name: str, exec: bool = False,
            type: T.Optional[type] = None,
            range: T.Optional[object] = None,
            default: T.Optional[object] = None,
            save_cache: bool = True,
            **kwargs,
            ) -> None:
        self.name = name
        self.exec = exec
        self.type = type
        self.range = range
        self.default = default
        self.save_cache = save_cache
        self.attrs = kwargs

    @classmethod
    def from_val_desc(cls, val_desc: "Value") -> "Port":
        port = cls(
            name=val_desc.name,
            type=val_desc.type,
            range=val_desc.range,
            default=val_desc.default,
            **val_desc.kwargs,
        )
        return port

    def to_val_desc(self) -> "Value":
        val_desc = Value(
            name=self.name,
            type_=self.type,
            range_=self.range,
            default=self.default,
            **self.attrs,
        )
        return val_desc

    def to_input_port(self, node: "Node") -> InputPort:
        port: InputPort
        if self.exec:
            port = InputExecPort(self.name, node)
        else:
            port = InputDataPort(self.name, node, self.to_val_desc())
        return port

    def to_output_port(self, node: "Node") -> OutputPort:
        port: OutputPort
        if self.exec:
            port = OutputExecPort(self.name, node)
        else:
            port = OutputDataPort(
                self.name, node, self.save_cache,
                self.to_val_desc())
        return port
