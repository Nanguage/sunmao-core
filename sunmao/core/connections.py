import typing as T

from .base import SunmaoObj


if T.TYPE_CHECKING:
    from .node_port import InputPort, OutputPort


class Connection(SunmaoObj):
    def __init__(self, source: "OutputPort", target: "InputPort") -> None:
        super().__init__()
        self.source = source
        self.target = target

    def __eq__(self, __o: "Connection") -> bool:
        return (self.source == __o.source) and (self.target == __o.target)
