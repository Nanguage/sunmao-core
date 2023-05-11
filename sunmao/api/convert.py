import typing as T
import functools
from funcdesc import parse_func

from ..core.node import ComputeNode
from ..core.node_port import Port
from ..core.utils import JOB_TYPES


def compute(
        target_func: T.Optional[T.Callable] = None,
        default_exec_mode: T.Literal['all', 'any'] = 'all',
        default_job_type: JOB_TYPES = 'thread',
        ) -> T.Type[ComputeNode]:
    """Decorator for create ComputeNode from a callable object."""
    if target_func is None:
        return functools.partial(
            compute,
            default_exec_mode=default_exec_mode,
            default_job_type=default_job_type,
        )  # type: ignore
    else:
        desc = parse_func(target_func)
        input_bps = [Port.from_val_desc(v) for v in desc.inputs]
        output_bps = [Port.from_val_desc(v) for v in desc.outputs]
        _default_exec_mode = default_exec_mode
        _default_job_type = default_job_type

        class Node(ComputeNode):
            __name__ = target_func.__name__  # type: ignore
            __doc__ = target_func.__doc__
            init_input_ports: T.List["Port"] = input_bps
            init_output_ports: T.List["Port"] = output_bps
            default_exec_mode = _default_exec_mode
            default_job_type = _default_job_type
            func = staticmethod(target_func)  # type: ignore
            func_desc = desc

        return Node
