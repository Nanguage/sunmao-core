import typing as T
import asyncio

from .node_port import PortBluePrint, OutputDataPort
from .channel import Channel


class Node(object):
    init_input_ports: T.List[PortBluePrint] = []
    init_output_ports: T.List[PortBluePrint] = []

    def __init__(self) -> None:
        self.setup_ports()

    def setup_ports(self):
        self.input_ports = [
            bp.to_input_port(self) for bp in self.init_input_ports
        ]
        self.output_ports = [
            bp.to_output_port(self) for bp in self.init_output_ports
        ]

    async def get_inputs(self) -> list:
        inputs = []
        for inp in self.input_ports:
            in_val = inp.get_val()
            inputs.append(in_val)
        inputs = await asyncio.gather(*inputs)
        return inputs

    async def push_output(self, idx: int, val: T.Any):
        port = self.output_ports[idx]
        if isinstance(port, OutputDataPort):
            await port.put_val(val)
        else:
            raise TypeError(
                f"port of node {self}(order: {idx}): "
                f"{port} is not OutputDataPort")

    async def activate_all_ports(self):
        calls = []
        for port in self.output_ports:
            calls.append(port.activate_successors())
        await asyncio.gather(*calls)

    async def run(self):
        pass

    def connect_with(
            self, other: "Node",
            self_port_idx: int, other_port_idx: int):
        out = self.output_ports[self_port_idx]
        in_ = other.input_ports[other_port_idx]
        ch = Channel()
        out.connect_channel(ch)
        in_.connect_channel(ch)

    def connect_channel(self, port_idx: int, channel: "Channel"):
        port = self.output_ports[port_idx]
        port.connect_channel(channel)


class ComputeNode(Node):
    @staticmethod
    def func(*args, **kwargs):
        pass

    @classmethod
    def as_func(cls):
        return cls.func

    async def run(self):
        inputs = await self.get_inputs()
        res = self.func(*inputs)
        routines = []
        if isinstance(res, tuple):
            # multiple output value
            for idx, r in enumerate(res):
                r = self.push_output(idx, r)
                routines.append(r)
        else:
            # single output value
            r = self.push_output(0, res)
            routines.append(r)
        await asyncio.gather(*routines)
        await self.activate_all_ports()
