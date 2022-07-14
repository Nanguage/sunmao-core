import typing as T

from .base import SunmaoObj
from .node_port import InputDataPort, InputExecPort
from .node_port import OutputDataPort, OutputExecPort


if T.TYPE_CHECKING:
    from .node_port import PortBluePrint


class Node(SunmaoObj):

    init_input_ports: T.List["PortBluePrint"] = []
    init_output_ports: T.List["PortBluePrint"] = []

    def __init__(self) -> None:
        super().__init__()
        self.setup_ports()

    def setup_ports(self):
        self.input_ports = [
            bp.to_input_port(self) for bp in self.init_input_ports
        ]
        self.output_ports = [
            bp.to_output_port(self) for bp in self.init_output_ports
        ]

    def activate(self):
        signal_bufs = [inp.signal_buffer for inp in self.input_ports]
        if all([len(buf) > 0 for buf in signal_bufs]):
            args = self.consume_signal()
            self.run(*args)

    def consume_signal(self) -> T.List[T.Any]:
        args = []
        for inp in self.input_ports:
            if isinstance(inp, InputDataPort):
                data = inp.get_data()
                args.append(data)
            else:
                assert isinstance(inp, InputExecPort)
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


class ComputeNode(Node):

    def run(self, *args) -> T.Any:
        res = self.func(*args)
        self.set_outputs(res)

    def set_outputs(self, res: T.Union[T.Tuple, T.Any]):
        if isinstance(res, tuple):
            for i, r in enumerate(res):
                self.set_output(i, r)
        else:
            self.set_output(0, res)

    def get_output_caches(self) -> T.List[T.Any]:
        res = []
        for o in self.output_ports:
            if isinstance(o, OutputDataPort):
                r = o.get_cache()
                res.append(r)
        return res

    def __call__(self, *args):
        _args = list(args)
        for inp in self.input_ports:
            if isinstance(inp, InputDataPort):
                a = _args.pop(0)
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

    @staticmethod
    def func(*args):
        pass
