import typing as T
import pytest
import time

from sunmao.core.node import ComputeNode
from sunmao.core.node_port import Port
from sunmao.core.error import TypeCheckError, RangeCheckError
from sunmao.core.connection import Connection
from sunmao.core.flow import Flow
from sunmao.core.session import Session
from executor.engine import EngineSetting


@pytest.fixture
def node_defs():
    class AddNode(ComputeNode):
        init_input_ports = [
            Port("a", data_type=int, data_range=(0, 100)),
            Port("b", data_type=int, data_range=(0, 100)),
        ]
        init_output_ports = [
            Port("res", data_type=int, data_range=(0, 100))
        ]

        @staticmethod
        def func(a: int, b: int) -> int:
            return a + b

    class AddNodeDefault(AddNode):
        init_input_ports = [
            Port("a", data_type=int, data_range=(0, 100)),
            Port(
                "b", data_type=int, data_range=(0, 100),
                data_default=10),
        ]

    class SquareNode(ComputeNode):
        init_input_ports = [
            Port("a")
        ]
        init_output_ports = [
            Port("res")
        ]

        @staticmethod
        def func(a):
            return a**2

    class SleepSquareNode(SquareNode):
        @staticmethod
        def func(a):
            time.sleep(0.5)
            return a**2

    class LongSleepSquare(SquareNode):
        @staticmethod
        def func(a):
            time.sleep(10)
            return a**2

    _node_defs = {}

    _node_defs['add'] = AddNode
    _node_defs['add_with_default'] = AddNodeDefault
    _node_defs['square'] = SquareNode
    _node_defs['sleep_square'] = SleepSquareNode
    _node_defs['long_sleep_square'] = LongSleepSquare
    return _node_defs


def test_flow(node_defs):
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


def test_one_node_run(node_defs):
    with Session() as sess:
        Add: T.Type[ComputeNode] = node_defs['add']
        add: ComputeNode = Add(job_type='local')
        job = add(1, 2)
        sess.engine.wait_job(job)
        assert job.result() == 3
        with pytest.raises(TypeCheckError):
            add(1.0, 2)
        with pytest.raises(RangeCheckError):
            add(1, 101)


def test_job_join():
    Add = node_defs['add']
    for exe in ("local", "thread", "process"):
        add: ComputeNode = Add(executor=exe)
        job = add(1, 2)
        job.join()
        assert job.result == 3


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
    add0: ComputeNode = Add(executor="local")
    add1: ComputeNode = Add(executor="local")
    add2: ComputeNode = Add(executor="local")
    add0.connect_with(add2, 0, 0)
    add1.connect_with(add2, 0, 0)
    add1.connect_with(add2, 0, 0)  # repeat connect
    add1.connect_with(add2, 0, 1)
    assert len(add1.output_ports[0].connections) == 2
    assert len(add2.input_ports[0].connections) == 1
    add1(1, 2)
    assert add2.output_ports[0].cache == 6
    add0.output_ports[0].disconnect(add2.input_ports[0])
    Square = node_defs['square']
    sq1: ComputeNode = Square(executor="local")
    sq2: ComputeNode = Square(executor="local")
    # chain connect
    add0.connect_with(sq1, 0, 0).connect_with(sq2, 0, 0)
    add0(1, 1)
    assert sq2.output_ports[0].cache == 16


def test_exec_mode():
    Add = node_defs['add']
    add0: ComputeNode = Add(executor="local")
    add1: ComputeNode = Add(executor="local")
    add2: ComputeNode = Add(executor="local")
    add0.connect_with(add2, 0, 0)
    add1.connect_with(add2, 0, 1)
    add0(1, 1)
    assert add2.output_ports[0].cache is None
    add1(1, 1)
    assert add2.output_ports[0].cache == 4
    add0(2, 2)
    assert add2.output_ports[0].cache == 4
    add2.exec_mode = 'any'
    add0(2, 2)
    assert add2.output_ports[0].cache == 6
    add2.clear_port_caches()
    assert add2.output_ports[0].cache is None


def test_port_default():
    AddDefault = node_defs['add_with_default']
    add1: ComputeNode = AddDefault(executor="local")
    add1(1)
    assert add1.output_ports[0].cache == 11
    add1(a=5)
    assert add1.output_ports[0].cache == 15
    add1(b=5, a=1)
    assert add1.output_ports[0].cache == 6
    add2: ComputeNode = AddDefault(executor="local")
    add2.exec_mode = 'any'
    add1.connect_with(add2, 0, 0)
    add1(2, 2)
    assert add2.output_ports[0].cache == 14


def test_thread_executor():
    sess = Session()
    SleepSq = node_defs['sleep_square']
    sq1: ComputeNode = SleepSq(executor="thread")
    sq1(3)
    sess.engine.wait()
    assert sq1.output_ports[0].cache == 9
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
    assert (t2 - t1) < 1.0
    assert add.output_ports[0].cache == 50


def test_thread_executor_resource_consume():
    sess = Session(engine_setting=EngineSetting(max_threads=1))
    SleepSq = node_defs['sleep_square']
    sq1: ComputeNode = SleepSq(executor="thread")
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
    assert (t2 - t1) > 1.0
    assert add.output_ports[0].cache == 50


def test_process_executor():
    sess = Session()
    SleepSq = node_defs['sleep_square']
    sq1: ComputeNode = SleepSq(executor="process")
    sq1(3)
    sess.engine.wait()
    assert sq1.output_ports[0].cache == 9


def test_process_executor_resource_consume():
    sess = Session(engine_setting=EngineSetting(max_processes=1))
    SleepSq = node_defs['sleep_square']
    sq1: ComputeNode = SleepSq(executor="process")
    Add = node_defs['add']
    add: ComputeNode = Add(executor="process")
    sq2: ComputeNode = SleepSq(executor="process")
    sq1.connect_with(add, 0, 0)
    sq2.connect_with(add, 0, 1)
    sq1(5)
    assert sess.engine.process_count == 0
    sq2(5)
    sess.engine.wait()
    assert add.output_ports[0].cache == 50


def test_job_cancel():
    from copy import copy
    sess = Session()
    LongSleepSq = node_defs['long_sleep_square']
    thread_count = copy(sess.engine.thread_count)
    sq1: ComputeNode = LongSleepSq(executor="thread")
    sq1(3)
    assert len(sess.engine.jobs.running) == 1
    j = list(sess.engine.jobs.running.values())[0]
    j.cancel()
    assert len(sess.engine.jobs.running) == 0
    assert j.status == "canceled"
    assert thread_count == sess.engine.thread_count
    # test process job cancel
    sq1.executor = "process"
    sq1(3)
    assert len(sess.engine.jobs.running) == 1
    time.sleep(0.1)
    j = list(sess.engine.jobs.running.values())[0]
    j.cancel()
    assert len(sess.engine.jobs.running) == 0
    assert j.status == "canceled"


def test_job_re_emit():
    sess = Session()
    SleepSq = node_defs['sleep_square']
    sq1: ComputeNode = SleepSq(executor="thread")
    sq1(3)
    sess.engine.wait()
    assert sq1.output_ports[0].cache == 9
    sq1.clear_port_caches()
    assert len(sess.engine.jobs.done) == 1
    j = list(sess.engine.jobs.done.values())[0]
    j.emit()
    sess.engine.wait()
    assert sq1.output_ports[0].cache == 9


def test_attr_range():
    sess = Session()
    SleepSq = node_defs['sleep_square']
    sq1: ComputeNode = SleepSq(executor="local")
    with pytest.raises(RangeCheckError):
        sq1.exec_mode = "aaa"
    with pytest.raises(RangeCheckError):
        sq1.executor = "aaa"
    sq1(3)
    j = list(sess.engine.jobs.done.values())[0]
    with pytest.raises(RangeCheckError):
        j.status = "aaaa"
