import pytest
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
        await Session.get_current().join()


@pytest.mark.asyncio
async def test_api_3():
    @compute
    @mark_output(0, type=str)
    @mark_output(1, type=int)
    def Test(a: int):
        return 'ok', a

    t = Test()
    await t(1)
    await Session.get_current().join()
    assert t.caches == ('ok', 1)


@pytest.mark.asyncio
async def test_api_4():
    @compute
    def Square(a: int) -> int:
        return a ** 2

    with Session() as sess:
        sq1 = Square()
        sq2 = Square()
        sq3 = Square()
        sq1 >> sq2 >> sq3
        await sq1(2)
        await sess.join()
        assert sq3.O[0].cache == ((2**2)**2)**2


@pytest.mark.asyncio
async def test_long_chain_join():
    @compute
    def Inc(a: int) -> int:
        return a + 1

    with Session() as sess:
        chain_len = 10
        incs = [Inc() for _ in range(chain_len)]
        for i in range(chain_len - 1):
            incs[i] >> incs[i + 1]
        await incs[0](0)
        await sess.join()
        assert incs[-1].O[0].cache == chain_len


@pytest.mark.asyncio
async def test_compute_node_output_cache():
    @compute(save_output_cache=False)
    def Inc(a: int) -> int:
        return a + 1

    with Session() as sess:
        inc = Inc()
        await inc(0)
        await sess.join()
        assert inc.O[0].cache is None
