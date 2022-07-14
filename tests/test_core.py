import pytest
from sunmao.core.node import ComputeNode
from sunmao.core.node_port import PortBluePrint
from sunmao.core.base import TypeCheckError, RangeCheckError


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

    node_defs['add'] = AddNode


def test_one_node_run():
    Add: ComputeNode = node_defs['add']
    add = Add()
    assert add(1, 2) == 3
    with pytest.raises(TypeCheckError):
        add(1.0, 2)
    with pytest.raises(RangeCheckError):
        add(1, 101)
    with pytest.raises(RangeCheckError):
        add(100, 100)
