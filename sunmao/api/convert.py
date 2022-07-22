import typing as T
import inspect
import functools

from ..core.node import ComputeNode
from ..core.node_port import PortBluePrint
from ..core.error import SunmaoError


class ConvertAPIError(SunmaoError):
    pass


def _class_getitem(cls, key: T.Tuple) -> PortBluePrint:
    name = 'port'
    type_ = object
    range_ = None
    if len(key) == 1:
        type_ = key[0]
    elif len(key) == 2:
        type_, range_ = key
    elif len(key) == 3:
        name, type_, range_ = key
    else:
        raise ConvertAPIError(
            f"Subscription on {cls.__name__} class only "
            "support less than 4 parameters. "
            f"Got: {len(key)}")
    bp = PortBluePrint(
        name=name, data_type=type_, data_range=range_)
    return bp


class In(object):
    """Use for mark node input's type and range.

    Example
    -------
    ```
    @compute
    def func1(a: In[int, [0, 10]]) -> int:
        return a ** 2
    ```
    """
    __class_getitem__ = classmethod(_class_getitem)


class Out(object):
    """Use for mark node output's type and range.

    Example
    -------
    ```
    @compute
    def func1(a: int) -> Out[int, [0, 100]]:
        return a**2

    @compute
    def func2(a: int) -> Out["res", int, [0, 100]]:
        return a**2

    @compute
    def func3(a: int) -> Outputs[str, Out[0, 100]]:
        return "ok", a + 1
    ```
    """
    __class_getitem__ = classmethod(_class_getitem)


class Outputs(object):

    """Use for mark multiple node outputs.

    Example
    -------
    ```
    @compute
    def func2(a: int) -> Outputs[str, int]:
        return "ok", a + 1
    ```
    """
    def __class_getitem__(cls, key: T.Tuple) -> T.Tuple:
        return key


def parse_input_args(func: T.Callable) -> T.List[PortBluePrint]:
    sig = inspect.signature(func)
    bps = list()
    for n, p in sig.parameters.items():
        ann = p.annotation
        if isinstance(ann, PortBluePrint):
            bp = ann
        else:
            bp = PortBluePrint(n, data_type=ann)
        bp.name = n
        if p.default != inspect._empty:
            bp.data_default = p.default
        bps.append(bp)
    return bps


def parse_output_args(func: T.Callable) -> T.List[PortBluePrint]:
    ret_ann = func.__annotations__.get('return')
    bps = list()

    def construct_bp(ann, name="out"):
        if isinstance(ann, PortBluePrint):
            bp = ann
            bp.name = name
        else:
            bp = PortBluePrint(name=name, data_type=ann)
        return bp

    if isinstance(ret_ann, tuple):
        for idx, ann in enumerate(ret_ann):
            name = f"out{idx}"
            bp = construct_bp(ann, name)
            bps.append(bp)
    else:
        name = "out"
        bp = construct_bp(ret_ann, name)
        bps.append(bp)
    return bps


def compute(
        target_func: T.Optional[T.Callable], **kwargs) -> T.Type[ComputeNode]:
    """Decorator for create ComputeNode from a callable object."""
    if target_func is None:
        return functools.partial(compute, **kwargs)

    input_bps = parse_input_args(target_func)
    output_bps = parse_output_args(target_func)

    class Node(ComputeNode):
        __name__ = target_func.__name__
        __doc__ = target_func.__doc__
        init_input_ports: T.List["PortBluePrint"] = input_bps
        init_output_ports: T.List["PortBluePrint"] = output_bps
        default_exec_mode = kwargs.get("default_exec_mode", "all")
        default_executor = kwargs.get("default_executor", "thread")
        func = staticmethod(target_func)

    return Node
