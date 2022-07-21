import typing as T


class CheckAttrRange(object):
    valid_range: T.List[T.Any] = []
    attr = "__"

    def __get__(self, obj, objtype=None):
        return getattr(obj, self.attr)

    def __set__(self, obj, value):
        assert value in self.valid_range
        setattr(obj, self.attr, value)


Checker = T.Callable[[T.Any], bool]


class CheckAttrType(object):
    valid_type: T.List[T.Union[Checker, type]] = []
    attr = "__"

    def __get__(self, obj, objtype=None):
        return getattr(obj, self.attr)

    def __set__(self, obj, value):
        check_passed = []
        for tp in self.valid_type:
            if isinstance(tp, type):
                passed = isinstance(value, tp)
            else:
                assert isinstance(tp, T.Callable)
                passed = tp(value)
            check_passed.append(passed)
        is_valid_type = any(check_passed)
        assert is_valid_type
        setattr(obj, self.attr, value)
