import typing as T

from executor.engine.job import Job
from funcdesc import Description

from .base import FlowElement
from .node_port import (
    InputPort, OutputPort,
    InputDataPort, InputExecPort,
    OutputDataPort, OutputExecPort,
)
from .utils import CheckAttrRange, job_type_classes, JOB_TYPES
from .utils import logger


if T.TYPE_CHECKING:
    from .node_port import Port
    from .flow import Flow


class ExecMode(CheckAttrRange):
    valid_range = ("any", "all")
    attr = "_exec_mode"

    def __set__(self, obj: "Node", value: str):
        super().__set__(obj, value)
        obj.clear_signal_buffers()


class Node(FlowElement):
    """Base class of all nodes.

    Args:
        exec_mode (str, optional): Execution mode of the node.
            If "all", the node will be activated only when all input ports
            has signal. If "any", the node will be activated when any input
            port has signal. Defaults to "all".
        name (str, optional): Name of the node. Defaults to None.
        flow (Flow, optional): Flow that the node belongs to.
        **kwargs: Other attributes of the node.

    Attributes:
        input_ports (List[InputPort]): Input ports of the node.
        output_ports (List[OutputPort]): Output ports of the node.
        exec_mode (str): Execution mode of the node.
        name (str): Name of the node.
        jobs_id (List[str]): Ids of all jobs that the node submitted.
    """
    _instances_count = 0

    init_input_ports: T.List["Port"] = []
    init_output_ports: T.List["Port"] = []

    default_exec_mode: T.Literal['all', 'any'] = "all"
    exec_mode = ExecMode()

    def __init__(
            self,
            exec_mode: str = default_exec_mode,
            name: T.Optional[str] = None,
            flow: T.Optional["Flow"] = None,
            **kwargs
            ) -> None:
        super().__init__(flow=flow)
        self.setup_ports()
        self.exec_mode = exec_mode
        if name is None:
            name = self._get_name()
        self.name = name
        self.jobs_id: T.List[str] = []
        self.attrs = kwargs

    def copy(self, name: T.Optional[str] = None) -> "Node":
        """Return a copy of the node."""
        new_name = self.name if name is None else name
        node = self.__class__(
            exec_mode=self.exec_mode,
            name=new_name,
        )
        return node

    def _get_name(self) -> str:
        """Return a name for the node."""
        cls = self.__class__
        count = cls._instances_count
        name = f"{cls.__name__}_{count}"
        cls._instances_count += 1
        return name

    def __repr__(self) -> str:
        return f"<Node type={self.__class__.__name__} id={self.id}>"

    def setup_ports(self):
        self.input_ports = [
            bp.to_input_port(self) for bp in self.init_input_ports
        ]
        self.output_ports = [
            bp.to_output_port(self) for bp in self.init_output_ports
        ]

    def clear_signal_buffers(self):
        """Clear all signal buffers of input ports."""
        for inp in self.input_ports:
            inp.clear_signal_buffer()

    def clear_port_caches(self):
        """Clear all caches of output ports."""
        for p in self.output_ports:
            if isinstance(p, OutputDataPort):
                p.clear_cache()

    @property
    def caches(self) -> T.Union[T.Tuple, T.Any]:
        """Return caches of all output ports."""
        caches = tuple([
            p.cache for p in self.output_ports
            if isinstance(p, OutputDataPort)
        ])
        return caches

    async def activate(self):
        bufs_has_signal = [
            len(inp.signal_buffer) > 0 for inp in self.input_ports
        ]
        if self.exec_mode == "all":
            if all(bufs_has_signal):
                logger.info(f"{self} activated.")
                args = self.consume_all_ports()
                await self.run(*args)
        else:
            if any(bufs_has_signal):
                logger.info(f"{self} activated.")
                args = self.consume_ports_with_cache()
                await self.run(*args)

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

    async def set_output(self, idx: int, data: T.Any = None):
        """Set the cache of output port with index `idx` to `data`."""
        port = self.output_ports[idx]
        if isinstance(port, OutputDataPort):
            await port.push_signal(data=data)
        else:
            assert isinstance(port, OutputExecPort)
            await port.push_signal()

    async def set_outputs(self, res: T.Union[T.Tuple, T.Any]):
        if isinstance(res, tuple):
            for i, r in enumerate(res):
                await self.set_output(i, r)
        else:
            await self.set_output(0, res)

    async def run(self, *args):
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
            _args.append(inp.val_desc.default)
            idx += 1
        for idx, a in enumerate(args):
            _args[idx] = a
        for k, a in kwargs.items():
            idx = name_to_idx[k]
            _args[idx] = a
        return _args

    @property
    def free_input_ports(self) -> T.List["InputPort"]:
        """Return all input ports that not connected."""
        ports = []
        for inp in self.input_ports:
            if len(inp.connections) == 0:
                ports.append(inp)
        return ports

    @property
    def free_output_ports(self) -> T.List["OutputPort"]:
        """Return all output ports that not connected."""
        ports = []
        for outp in self.output_ports:
            if len(outp.connections) == 0:
                ports.append(outp)
        return ports

    async def __call__(self, *args, **kwargs):
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

    default_job_type: JOB_TYPES = "thread"
    job_type = JobType()
    func_desc: Description

    def __init__(
            self,
            exec_mode: str = Node.default_exec_mode,
            name: T.Optional[str] = None,
            job_type: JOB_TYPES = default_job_type,
            **kwargs) -> None:
        super().__init__(exec_mode=exec_mode, name=name, **kwargs)
        self.job_type = job_type  # type: ignore

    def copy(self, name: T.Optional[str] = None) -> "ComputeNode":
        node: ComputeNode = super().copy(name=name)  # type: ignore
        node.job_type = self.job_type
        return node

    def __repr__(self) -> str:
        return f"<ComputeNode type={self.__class__.__name__} id={self.id}>"

    @staticmethod
    async def callback(flow_id: str, node_id: str, res):
        from .session import Session
        sess = Session.get_current()
        node = sess.flows[flow_id].nodes[node_id]
        await node.set_outputs(res)

    @staticmethod
    async def error_callback(flow_id: str, node_id: str, e: Exception):
        print(str(e))

    async def run(self, *args) -> "Job":
        if self.flow is None:
            raise RuntimeError("Node not in a flow.")
        job_cls: T.Type[Job]
        job_cls = job_type_classes[self.job_type]
        flow_id = self.flow.id
        node_id = self.id
        _callback = self.callback
        _error_callback = self.error_callback
        _func = self.func

        async def callback(res):
            await _callback(flow_id, node_id, res)

        async def error_callback(e):
            await _error_callback(flow_id, node_id, e)

        def func(*args):
            return _func(*args)

        func.__name__ = self.__class__.__name__ + ".func"

        job = job_cls(
            func, args, name=self.__class__.__name__,
            callback=callback,
            error_callback=error_callback,
        )
        await self.session.engine.submit_async(job)
        self.jobs_id.append(job.id)
        return job

    async def __call__(self, *args, **kwargs) -> "Job":
        _args = self._get_call_args(*args, **kwargs)
        idx = 0
        for inp in self.input_ports:
            if isinstance(inp, InputDataPort):
                inp.check(_args[idx])
                idx += 1
        job = await self.run(*_args)
        return job

    @staticmethod
    def func(*args):
        pass
