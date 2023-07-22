import asyncio
from sunmao.api import compute, Session, Flow


@compute
def Add(a: int, b: int) -> int:
    return a + b


@compute
def Square(a: int) -> int:
    return a ** 2


async def main():
    with Flow():
        add = Add()
        sq1 = Square()
        sq2 = Square()
        sq1.O[0] >> add.I[0]
        sq2.O[0] >> add.I[1]
    await sq1(10)
    await sq2(10)
    await Session.get_current().engine.join()
    assert add.O[0].cache == 200


if __name__ == '__main__':
    asyncio.run(main())
