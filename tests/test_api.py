from sunmao.api import compute, In, Out, Outputs, Session


def test_api():
    @compute
    def Add(a: int, b: int) -> int:
        return a + b

    @compute
    def Square(a: int) -> int:
        return a ** 2
    
    sess = Session()
    add = Add()
    sq1 = Square()
    sq2 = Square()
    sq1 >> add.I[0]
    sq2 >> add.I[1]
    sq1(10)
    sq2(10)
    sess.engine.wait()
    assert add.O[0].cache == 200
