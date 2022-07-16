class CheckAttrSet(object):
    valid = []
    attr = "__"

    def __get__(self, obj, objtype=None):
        return getattr(obj, self.attr)

    def __set__(self, obj, value):
        assert value in self.valid
        setattr(obj, self.attr, value)
