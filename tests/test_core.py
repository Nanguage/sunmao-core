import typing as T
import pytest
import time

from sunmao.core.node import ComputeNode
from sunmao.core.node_port import Port
from sunmao.core.connection import Connection
from sunmao.core.flow import Flow
from sunmao.core.session import Session


@pytest.fixture
def node_defs():
    class AddNode(ComputeNode):
        init_input_ports = [
            Port("a", type=int, range=(0, 100)),
            Port("b", type=int, range=(0, 100)),
        ]
        init_output_ports = [
            Port("res", type=int, range=(0, 100))
        ]

        @staticmethod
        def func(a: int, b: int) -> int:
            return a + b

    class AddNodeDefault(AddNode):
        init_input_ports = [
            Port("a", type=int, range=(0, 100)),
            Port("b", type=int, range=(0, 100), default=10),
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
    with Flow() as flow1:
        add1: ComputeNode = Add(flow=flow1)
        assert add1.flow is flow1
        add2: ComputeNode = Add()
        assert add2.flow is flow1
    assert add.session.current_flow is add.flow
    with Flow() as flow2:
        add3: ComputeNode = Add()
        assert add3.flow is flow2
        add2.flow = flow2
        assert add2.flow is flow2
        assert add2.id not in flow1.nodes


def test_session():
    with Session() as sess1:
        flow = Flow(session=sess1)
        assert flow.session is sess1
        with Session() as sess2:
            flow1 = Flow()
            assert flow1.session is sess2
        flow2 = Flow()
        assert flow2.session is sess1


@pytest.mark.asyncio
async def test_one_node_run(node_defs):
    with Session():
        Add: T.Type[ComputeNode] = node_defs['add']
        add: ComputeNode = Add(job_type='local')
        job = await add(1, 2)
        await job.join()
        assert job.result() == 3
        with pytest.raises(TypeError):
            await add(1.0, 2)
        with pytest.raises(ValueError):
            await add(1, 101)


@pytest.mark.asyncio
async def test_job_join(node_defs):
    Add = node_defs['add']
    with Session():
        for j_type in ("local", "thread", "process"):
            add: ComputeNode = Add(job_type=j_type)
            job = await add(1, 2)
            await job.join()
            assert job.result() == 3


def test_conn_in_op(node_defs):
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


@pytest.mark.asyncio
async def test_node_connect(node_defs):
    Add = node_defs['add']
    with Session() as sess:
        add0: ComputeNode = Add(job_type="local")
        add1: ComputeNode = Add(job_type="local")
        add2: ComputeNode = Add(job_type="local")
        add0.connect_with(add2, 0, 0)
        add1.connect_with(add2, 0, 0)
        add1.connect_with(add2, 0, 0)  # repeat connect
        add1.connect_with(add2, 0, 1)
        assert len(add1.output_ports[0].connections) == 2
        assert len(add2.input_ports[0].connections) == 2
        await add1(1, 2)
        await sess.join()
        assert add2.output_ports[0].cache == 6
        add0.output_ports[0].disconnect(add2.input_ports[0])
        Square = node_defs['square']
        sq1: ComputeNode = Square(job_type="local")
        sq2: ComputeNode = Square(job_type="local")
        # chain connect
        add0.connect_with(sq1, 0, 0).connect_with(sq2, 0, 0)
        await add0(1, 1)
        await sess.join()
        assert sq2.output_ports[0].cache == 16


@pytest.mark.asyncio
async def test_exec_mode(node_defs):
    with Session() as sess:
        Add = node_defs['add']
        add0: ComputeNode = Add(job_type="local")
        add1: ComputeNode = Add(job_type="local")
        add2: ComputeNode = Add(job_type="local")
        add0.connect_with(add2, 0, 0)
        add1.connect_with(add2, 0, 1)
        await add0(1, 1)
        await sess.join()
        assert add2.output_ports[0].cache is None
        await add1(1, 1)
        await sess.join()
        assert add2.output_ports[0].cache == 4
        await add0(2, 2)
        await sess.join()
        assert add2.output_ports[0].cache == 4
        add2.exec_mode = 'any'
        await add0(2, 2)
        await sess.join()
        assert add2.output_ports[0].cache == 6
        add2.clear_port_caches()
        assert add2.output_ports[0].cache is None


@pytest.mark.asyncio
async def test_port_default(node_defs):
    AddDefault = node_defs['add_with_default']
    add1: ComputeNode = AddDefault(job_type="local")
    job = await add1(1)
    await job.join()
    assert add1.output_ports[0].cache == 11
    job = await add1(a=5)
    await job.join()
    assert add1.output_ports[0].cache == 15
    job = await add1(b=5, a=1)
    await job.join()
    assert add1.output_ports[0].cache == 6
    add2: ComputeNode = AddDefault(job_type="local")
    add2.exec_mode = 'any'
    add1.connect_with(add2, 0, 0)
    job = await add1(2, 2)
    await job.join()
    assert add2.output_ports[0].cache == 14


def test_attr_range(node_defs):
    from executor.engine.utils import RangeCheckError
    SleepSq = node_defs['sleep_square']
    sq1: ComputeNode = SleepSq(job_type="local")
    with pytest.raises(RangeCheckError):
        sq1.exec_mode = "aaa"
    with pytest.raises(RangeCheckError):
        sq1.job_type = "aaa"


def test_node_name(node_defs):
    with Flow() as flow:
        Add = node_defs['add']
        add1: ComputeNode = Add(name="add1")
        assert add1.name == "add1"
        assert len(flow.nodes) == 1
        add2: ComputeNode = Add()
        assert add2.name == "AddNode_1"


@pytest.mark.asyncio
async def test_flow_call(node_defs):
    with Flow() as flow:
        Add = node_defs['add']
        Square = node_defs['square']
        add1: ComputeNode = Add(name="add1")
        sq1: ComputeNode = Square(name="sq1")
        sq2: ComputeNode = Square(name="sq2")
        sq1.connect_with(add1, 0, 0)
        sq2.connect_with(add1, 0, 1)
        res = await flow({
            "sq1.a": 1,
            "sq2.a": 2,
        })
        assert res == {'add1.res': 5}


@pytest.mark.asyncio
async def test_node_output_port_cache():
    class Exchange(ComputeNode):
        init_input_ports = [
            Port("a"),
            Port("b"),
        ]
        init_output_ports = [
            Port("b", save_cache=False),
            Port("a", save_cache=True),
        ]

        @staticmethod
        def func(a, b):
            return b, a

    ex1 = Exchange()
    ex2 = Exchange()
    ex1.connect_with(ex2, 0, 0)
    ex1.connect_with(ex2, 1, 1)
    await ex1(1, 2)
    await Session.get_current().join()
    assert ex2.output_ports[0].cache is None
    assert ex2.output_ports[1].cache == 2


def test_node_copy(node_defs):
    Add = node_defs['add']
    add1: ComputeNode = Add(name="add1", exec_mode="any")
    add2 = add1.copy()
    assert add2.name == "add1_copy"
    add3 = add1.copy(name="add3")
    assert add3.name == "add3"
    assert add3.id != add1.id
    assert add3.flow is add1.flow
    assert add3.exec_mode == add1.exec_mode
