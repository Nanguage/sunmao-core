import typing as T

from executor.engine.job import Job

from .base import FlowElement
from .node_port import (
    InputDataPort, InputExecPort,
    OutputDataPort, OutputExecPort,
)
from .utils import CheckAttrRange, job_type_classes


if T.TYPE_CHECKING:
    from .node_port import Port


class ExecMode(CheckAttrRange):
    valid_range = ("any", "all")
    attr = "_exec_mode"

    def __set__(self, obj: "Node", value: str):
        super().__set__(obj, value)
        obj.clear_signal_buffers()


class Node(FlowElement):

    init_input_ports: T.List["Port"] = []
    init_output_ports: T.List["Port"] = []

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

    @property
    def caches(self) -> T.Union[T.Tuple, T.Any]:
        caches = tuple([
            p.cache for p in self.output_ports
            if isinstance(p, OutputDataPort)
        ])
        if len(caches) > 1:
            return caches
        else:
            return caches[0]

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

    def set_outputs(self, res: T.Union[T.Tuple, T.Any]):
        if isinstance(res, tuple):
            for i, r in enumerate(res):
                self.set_output(i, r)
        else:
            self.set_output(0, res)

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

    def _get_call_args(self, *args, **kwargs) -> T.List[T.Any]:
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


class JobType(CheckAttrRange):
    valid_range = ("local", "thread", "process", "dask")
    attr = "_job_type"


class ComputeNode(Node):

    default_job_type = "thread"
    job_type = JobType()

    def __init__(
            self,
            exec_mode: str = Node.default_exec_mode,
            job_type: str = default_job_type,
            **kwargs) -> None:
        super().__init__(exec_mode, **kwargs)
        self.job_type = job_type  # type: ignore

    @staticmethod
    def callback(flow_id: str, node_id: str, res):
        from .session import Session
        sess = Session.get_current()
        node = sess.flows[flow_id].nodes[node_id]
        node.set_outputs(res)

    @staticmethod
    def error_callback(flow_id: str, node_id: str, e: Exception):
        print(str(e))

    def run(self, *args) -> "Job":
        job_cls: T.Type[Job]
        job_cls = job_type_classes[self.job_type]
        flow_id = self.flow.id
        node_id = self.id
        _callback = self.callback
        _error_callback = self.error_callback
        job = job_cls(
            self.func, args, name=self.__class__.__name__,
            callback=lambda res: _callback(flow_id, node_id, res),
            error_callback=lambda e: _error_callback(flow_id, node_id, e),
        )
        self.session.engine.submit(job)
        return job

    def __call__(self, *args, **kwargs) -> "Job":
        _args = self._get_call_args(*args, **kwargs)
        idx = 0
        for inp in self.input_ports:
            if isinstance(inp, InputDataPort):
                inp.check(_args[idx])
                idx += 1
        job = self.run(*_args)
        return job

    @staticmethod
    def func(*args):
        pass
