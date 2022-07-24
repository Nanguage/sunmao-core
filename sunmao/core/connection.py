import typing as T

from .base import FlowElement


if T.TYPE_CHECKING:
    from .node_port import InputPort, OutputPort


class Connection(FlowElement):
    def __init__(
            self, source: "OutputPort", target: "InputPort",
            **kwargs,
            ) -> None:
        super().__init__(**kwargs)
        self.source = source
        self.target = target

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Connection): 
            return NotImplemented
        return (self.source == other.source) and (self.target == other.target)
