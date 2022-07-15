class SunmaoError(Exception):
    pass


class CheckError(SunmaoError):
    pass


class TypeCheckError(SunmaoError):
    pass


class RangeCheckError(SunmaoError):
    pass
