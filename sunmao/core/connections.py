import typing as T

from .base import SunmaoObj


if T.TYPE_CHECKING:
    from .node_port import InputPort, OutputPort


class Connection(SunmaoObj):
    def __init__(self, source: "OutputPort", target: "InputPort") -> None:
        super().__init__()
        self.source = source
        self.target = target
