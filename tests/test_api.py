import pytest
import asyncio
from sunmao.api import compute, Session
from funcdesc import mark_input, mark_output


@pytest.mark.asyncio
async def test_api():
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
    await sq1(10)
    await sq2(10)
    await Session.get_current().engine.join()
    assert add.O[0].cache == 200


@pytest.mark.asyncio
async def test_api_2():
    @compute
    @mark_input(0, range=[0, 10])
    def EqRet(a: int) -> int:
        return a

    eq = EqRet()
    with pytest.raises(ValueError):
        await eq(100)
        await Session.get_current().engine.join()


@pytest.mark.asyncio
async def test_api_3():
    @compute
    @mark_output(0, type=str)
    @mark_output(1, type=int)
    def Test(a: int):
        return 'ok', a

    t = Test()
    await t(1)
    await Session.get_current().engine.join()
    assert t.caches == ('ok', 1)


@pytest.mark.asyncio
async def test_api_4():
    @compute
    def Square(a: int) -> int:
        return a ** 2

    sq1 = Square()
    sq2 = Square()
    sq3 = Square()
    sq1 >> sq2 >> sq3
    await sq1(2)
    await asyncio.sleep(0.1)
    assert sq3.O[0].cache == ((2**2)**2)**2
