import typing as T

from .base import FlowElement
from .node_port import (
    InputDataPort, InputExecPort,
    OutputDataPort, OutputExecPort,
)
from .job import LocalJob, ThreadJob, ProcessJob
from .utils import CheckAttrRange


if T.TYPE_CHECKING:
    from .node_port import PortBluePrint


class ExecMode(CheckAttrRange):
    valid_range = ("any", "all")
    attr = "_exec_mode"

    def __set__(self, obj: "Node", value: str):
        super().__set__(obj, value)
        obj.clear_signal_buffers()


class Node(FlowElement):

    init_input_ports: T.List["PortBluePrint"] = []
    init_output_ports: T.List["PortBluePrint"] = []

    default_exec_mode = "all"
    exec_mode = ExecMode()

    def __init__(
            self,
            exec_mode: str = default_exec_mode,
            **kwargs
            ) -> None:
        super().__init__(**kwargs)
        self.setup_ports()
        self.exec_mode = exec_mode

    def setup_ports(self):
        self.input_ports = [
            bp.to_input_port(self) for bp in self.init_input_ports
        ]
        self.output_ports = [
            bp.to_output_port(self) for bp in self.init_output_ports
        ]

    def clear_signal_buffers(self):
        for inp in self.input_ports:
            inp.clear_signal_buffer()

    def clear_port_caches(self):
        for p in self.output_ports:
            if isinstance(p, OutputDataPort):
                p.clear_cache()

    def activate(self):
        bufs_has_signal = [
            len(inp.signal_buffer) > 0 for inp in self.input_ports
        ]
        if self.exec_mode == "all":
            if all(bufs_has_signal):
                args = self.consume_all_ports()
                self.run(*args)
        else:
            if any(bufs_has_signal):
                args = self.consume_ports_with_cache()
                self.run(*args)

    def consume_all_ports(self) -> T.List[T.Any]:
        """Consume one signal of all ports.
        Assume that all ports has at least one signal."""
        args = []
        for inp in self.input_ports:
            assert len(inp.signal_buffer) > 0
            if isinstance(inp, InputDataPort):
                data = inp.get_data()
                args.append(data)
            else:
                assert isinstance(inp, InputExecPort)
                inp.get_signal()
        return args

    def consume_ports_with_cache(self) -> T.List[T.Any]:
        """Consume one signal of all ports.
        If a InputDataPort not has signal,
        will replace with the predecessor's cache or
        it's default value.
        """
        args = []
        for inp in self.input_ports:
            if isinstance(inp, InputDataPort):
                if len(inp.signal_buffer) > 0:
                    data = inp.get_data()
                else:
                    data = inp.fetch_missing()
                args.append(data)
            else:
                assert isinstance(inp, InputExecPort)
                if len(inp.signal_buffer) > 0:
                    inp.get_signal()
        return args

    def set_output(self, idx: int, data: T.Any = None):
        port = self.output_ports[idx]
        if isinstance(port, OutputDataPort):
            port.push_data(data)
        else:
            assert isinstance(port, OutputExecPort)
            port.activate_successors()

    def run(self, *args):
        pass

    def connect_with(
            self, other: "Node",
            self_port_idx: int, other_port_idx: int) -> "Node":
        out_port = self.output_ports[self_port_idx]
        in_port = other.input_ports[other_port_idx]
        out_port.connect_with(in_port)
        return other

    def get_output_caches(self) -> T.List[T.Any]:
        res = []
        for o in self.output_ports:
            if isinstance(o, OutputDataPort):
                r = o.get_cache()
                res.append(r)
        return res

    def _get_call_args(self, *args, **kwargs):
        name_to_idx = {}
        _args = []
        idx = 0
        for inp in self.input_ports:
            if not isinstance(inp, InputDataPort):
                continue
            name_to_idx[inp.name] = idx
            _args.append(inp.default)
            idx += 1
        for idx, a in enumerate(args):
            _args[idx] = a
        for k, a in kwargs.items():
            idx = name_to_idx[k]
            _args[idx] = a
        return _args

    def __call__(self, *args, **kwargs):
        _args = list(reversed(self._get_call_args(*args, **kwargs)))
        for inp in self.input_ports:
            if isinstance(inp, InputDataPort):
                a = _args.pop(-1)
                inp.put_signal(data=a)
            else:
                inp.put_signal()
        self.activate()
        caches = self.get_output_caches()
        if len(caches) == 1:
            return caches[0]
        elif len(caches) > 1:
            return tuple(caches)
        else:
            return None


class NodeExecutor(CheckAttrRange):
    valid_range = ("local", "thread", "process", "dask")
    attr = "_executor"


class ComputeNode(Node):

    default_executor = "local"
    executor = NodeExecutor()

    def __init__(
            self,
            exec_mode: str = Node.default_exec_mode,
            executor: str = default_executor,
            **kwargs) -> None:
        super().__init__(exec_mode, **kwargs)
        self.executor = executor

    def run(self, *args) -> T.Any:
        if self.executor == "local":
            job_cls = LocalJob
        elif self.executor == "thread":
            job_cls = ThreadJob
        else:
            job_cls = ProcessJob

        def callback(res):
            self.set_outputs(res)

        def error_callback(e):
            print(e)

        job = job_cls(self.func, args, callback, error_callback)
        self.session.engine.submit(job)

    def set_outputs(self, res: T.Union[T.Tuple, T.Any]):
        if isinstance(res, tuple):
            for i, r in enumerate(res):
                self.set_output(i, r)
        else:
            self.set_output(0, res)

    @staticmethod
    def func(*args):
        pass
