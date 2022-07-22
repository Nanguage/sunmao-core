import typing as T

from ..core.node import Node
from ..core.node_port import NodePort, InputPort, OutputPort


NodeOrInputPort = T.Union[Node, InputPort]


def _port_connect_with(outp: OutputPort, other: NodeOrInputPort) -> Node:
    if isinstance(other, Node):
        inp = other.input_ports[0]
        res = other
    else:
        inp = other
        res = other.node
    outp.connect_with(inp)
    return res


def patch_node():
    def _get_input_ports(self: Node) -> T.List[InputPort]:
        return self.input_ports

    def _get_output_ports(self: Node) -> T.List[OutputPort]:
        return self.output_ports

    Node.I = property(_get_input_ports)  # noqa
    Node.O = property(_get_output_ports)  # noqa

    def _get_port_by_name(self: Node, key: str) -> NodePort:
        for p in self.input_ports + self.output_ports:
            p: NodePort
            if p.name == key:
                return p
        raise KeyError(f"No port named {key} in node {self}.")

    Node.__getitem__ = _get_port_by_name

    def _rshift_connect(self: Node, other: NodeOrInputPort) -> Node:
        outp = self.output_ports[0]
        return _port_connect_with(outp, other)

    Node.__rshift__ = _rshift_connect


def patch_node_port():
    def _rshift_connect(self: OutputPort, other: NodeOrInputPort) -> Node:
        return _port_connect_with(self, other)

    OutputPort.__rshift__ = _rshift_connect


def patch_all():
    patch_node()
    patch_node_port()
