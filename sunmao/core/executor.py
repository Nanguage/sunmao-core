import asyncio
from pathos.multiprocessing import Pool
from multiprocessing.pool import ThreadPool


class ProcessExecutor:
    def __init__(self, func, args) -> None:
        #self.pool = Pool(processes=1)
        self.pool = ThreadPool(1)
        self.func = func
        self.args = args

    def run(self):
        loop = asyncio.get_event_loop()
        fut = loop.create_future()

        def callback(res):
            fut.set_result(res)
            print(fut)
            print(fut.done())
        self._r = self.pool.apply_async(
            self.func, tuple(self.args),
            callback=callback
        )
        return fut
