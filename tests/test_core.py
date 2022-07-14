import pytest
from sunmao.core.node import ComputeNode
from sunmao.core.node_port import *
from sunmao.core.channel import Channel
import asyncio


def test_port_blue_print():
    bp1 = PortBluePrint("a", data_type=int)
    assert isinstance(bp1.to_input_port(None), InputDataPort)


node_defs = {}


@pytest.mark.order(0)
def test_node_def():
    global node_defs

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

    node_defs['Add'] = Add

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

    node_defs['Square'] = Square

    return node_defs


def test_one_node_run():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    Add = node_defs['Add']
    add_node: ComputeNode = Add()
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
    add_node.connect_channel(0, ch3)
    asyncio.run(add_node.run())
    async def check_val():
        res = await ch3.get_val()
        assert res == 3
    asyncio.run(check_val())


def test_two_node_run():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    Add, Square = node_defs['Add'], node_defs['Square']
    add: ComputeNode = Add()
    sq: ComputeNode = Square()
    ch1 = Channel()
    ch2 = Channel()
    ch3 = Channel()
    # build call graph
    ch1.connect_with_node(add, 0)
    ch2.connect_with_node(add, 1)
    add.connect_with(sq, 0, 0)
    sq.connect_channel(0, ch3)
    # put data and run
    async def run():
        await asyncio.gather(
            ch1.put_val(1),
            ch2.put_val(2),
        )
        await add.run()
        res = await ch3.get_val()
        assert res == 9
    asyncio.run(run())


def test_executors():
    Add = node_defs['Add']
    add_node: ComputeNode = Add()
    for executor in ("none", "process"):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        add_node.set_executor(executor)
        ch1 = Channel()
        ch2 = Channel()
        ch3 = Channel()
        ch1.connect_with_node(add_node, 0)
        ch2.connect_with_node(add_node, 1)
        add_node.connect_channel(0, ch3)
        async def run():
            await asyncio.gather(
                ch1.put_val(1),
                ch2.put_val(2),
            )
            await add_node.run()
            res = await ch3.get_val()
            assert res == 3
        asyncio.run(run())


if __name__ == "__main__":
    test_node_def()
    test_executors()
