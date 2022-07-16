import pytest
import time

from sunmao.core.node import ComputeNode
from sunmao.core.node_port import PortBluePrint
from sunmao.core.error import TypeCheckError, RangeCheckError
from sunmao.core.connection import Connection
from sunmao.core.flow import Flow
from sunmao.core.session import Session


node_defs = {}


@pytest.mark.order(0)
def test_node_def():
    global node_defs

    class AddNode(ComputeNode):
        init_input_ports = [
            PortBluePrint("a", data_type=int, data_range=(0, 100)),
            PortBluePrint("b", data_type=int, data_range=(0, 100)),
        ]
        init_output_ports = [
            PortBluePrint("res", data_type=int, data_range=(0, 100))
        ]

        @staticmethod
        def func(a: int, b: int) -> int:
            return a + b

    class SquareNode(ComputeNode):
        init_input_ports = [
            PortBluePrint("a")
        ]
        init_output_ports = [
            PortBluePrint("res")
        ]

        @staticmethod
        def func(a):
            return a**2

    class SleepSquareNode(SquareNode):
        @staticmethod
        def func(a):
            time.sleep(1)
            return a**2

    node_defs['add'] = AddNode
    node_defs['square'] = SquareNode
    node_defs['sleep_square'] = SleepSquareNode


def test_flow():
    Add = node_defs['add']
    add: ComputeNode = Add()
    assert isinstance(add.flow, Flow)
    flow = Flow()
    add1: ComputeNode = Add(flow=flow)
    assert add1.flow is flow
    add2: ComputeNode = Add()
    assert add2.flow is flow


def test_session():
    sess = Session()
    flow = Flow(session=sess)
    assert flow.session is sess
    sess1 = Session()
    flow1 = Flow()
    assert flow1.session is sess1


def test_one_node_run():
    Add = node_defs['add']
    add: ComputeNode = Add()
    assert add(1, 2) == 3
    with pytest.raises(TypeCheckError):
        add(1.0, 2)
    with pytest.raises(RangeCheckError):
        add(1, 101)
    with pytest.raises(RangeCheckError):
        add(100, 100)


def test_conn_in_op():
    Add = node_defs['add']
    add1: ComputeNode = Add()
    add2: ComputeNode = Add()
    conns = [
        Connection(add1.output_ports[0], add2.input_ports[0]),
        Connection(add1.output_ports[0], add2.input_ports[1]),
    ]
    conn1 = Connection(add1.output_ports[0], add2.input_ports[0])
    assert conn1 in conns
    conns.remove(conn1)
    assert len(conns) == 1


def test_node_connect():
    Add = node_defs['add']
    add0: ComputeNode = Add()
    add1: ComputeNode = Add()
    add2: ComputeNode = Add()
    add0.connect_with(add2, 0, 0)
    add1.connect_with(add2, 0, 0)
    add1.connect_with(add2, 0, 0)  # repeat connect
    add1.connect_with(add2, 0, 1)
    assert len(add1.output_ports[0].connections) == 2
    assert len(add2.input_ports[0].connections) == 1
    add1(1, 2)
    assert add2.output_ports[0].get_cache() == 6
    add0.output_ports[0].disconnect(add2.input_ports[0])
    Square = node_defs['square']
    sq1: ComputeNode = Square()
    sq2: ComputeNode = Square()
    # chain connect
    add0.connect_with(sq1, 0, 0).connect_with(sq2, 0, 0)
    add0(1, 1)
    assert sq2.output_ports[0].get_cache() == 16


def test_exec_mode():
    Add = node_defs['add']
    add0: ComputeNode = Add()
    add1: ComputeNode = Add()
    add2: ComputeNode = Add()
    add0.connect_with(add2, 0, 0)
    add1.connect_with(add2, 0, 1)
    add0(1, 1)
    assert add2.output_ports[0].get_cache() is None
    add1(1, 1)
    assert add2.output_ports[0].get_cache() == 4
    add0(2, 2)
    assert add2.output_ports[0].get_cache() == 4
    add2.exec_mode = 'any'
    add0(2, 2)
    assert add2.output_ports[0].get_cache() == 6
    add2.clear_port_caches()
    assert add2.output_ports[0].get_cache() is None


def test_thread_executor():
    sess = Session()
    SleepSq = node_defs['sleep_square']
    sq1: ComputeNode = SleepSq(executor="thread")
    sq1(3)
    sess.engine.wait()
    assert sq1.output_ports[0].get_cache() == 9
    Add = node_defs['add']
    add: ComputeNode = Add(executor="thread")
    sq2: ComputeNode = SleepSq(executor="thread")
    sq1.connect_with(add, 0, 0)
    sq2.connect_with(add, 0, 1)
    t1 = time.time()
    sq1(5)
    sq2(5)
    sess.engine.wait()
    t2 = time.time()
    assert (t2 - t1) < 2.0
    assert add.output_ports[0].get_cache() == 50
