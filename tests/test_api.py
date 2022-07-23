import pytest
from sunmao.api import compute, In, Out, Outputs, Session
from sunmao.core.error import RangeCheckError


def test_api():
    @compute
    def Add(a: int, b: int) -> int:
        return a + b

    @compute
    def Square(a: int) -> int:
        return a ** 2
    
    add = Add()
    sq1 = Square()
    sq2 = Square()
    sq1 >> add.I[0]
    sq2 >> add.I[1]
    sq1(10)
    sq2(10)
    Session.get_current().wait()
    assert add.O[0].cache == 200


def test_api_2():
    @compute
    def EqRet(a: In[int, [0, 10]]) -> Out[int, [0, 10]]:
        return a

    eq = EqRet()
    with pytest.raises(RangeCheckError):
        eq(100)
        Session.get_current().wait()


def test_api_3():
    @compute
    def Test(a: int) -> Outputs[str, Out[int, [0, 10]]]:
        return 'ok', a

    t = Test()
    t(1)
    Session.get_current().wait()
    assert t.caches == ('ok', 1)
