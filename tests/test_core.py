from sunmao.core.node import ComputeNode
from sunmao.core.node_port import *
from sunmao.core.channel import Channel
import asyncio


def test_port_blue_print():
    bp1 = PortBluePrint("a", data_type=int)
    assert isinstance(bp1.to_input_port(None), InputDataPort)


def test_node_def():
    class Add(ComputeNode):
        init_input_ports = [
            PortBluePrint("a", data_type=int),
            PortBluePrint("b", data_type=int),
        ]
        init_output_ports = [
            PortBluePrint("res", data_type=int)
        ]

        @staticmethod
        def func(a: int, b: int) -> int:
            return a + b

    class Square(ComputeNode):
        init_input_ports = [
            PortBluePrint("a", data_type=int)
        ]
        init_output_ports = [
            PortBluePrint("res", data_type=int)
        ]
        @staticmethod
        def func(a: int) -> int:
            return a * a

    node_defs = {
        'Add': Add,
        'Square': Square
    }
    return node_defs


def test_one_node_run():
    node_defs = test_node_def()
    Add = node_defs['Add']
    add_node = Add()
    ch1 = Channel()
    ch2 = Channel()
    ch3 = Channel()
    async def append_vals():
        await asyncio.gather(
            ch1.put_val(1),
            ch2.put_val(2),
        )
    asyncio.run(append_vals())
    ch1.connect_with_node(add_node, 0)
    ch2.connect_with_node(add_node, 1)
    add_node.input_ports[0].connect_channel(ch1)
    add_node.input_ports[1].connect_channel(ch2)
    add_node.connect_channel(0, ch3)
    asyncio.run(add_node.run())
    async def check_val():
        res = await ch3.get_val()
        assert res == 3
    asyncio.run(check_val())


def test_two_node_run():
    pass
