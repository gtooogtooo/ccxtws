import asyncio
from abc import ABCMeta, abstractmethod
from . import logutils

logger = logutils.get_logger('ccxtws')


# https://refactoringguru.cn/design-patterns/observer/python/example
class ExchangeBoost(metaclass=ABCMeta):
    @abstractmethod
    def subscribe(self, observer):
        pass

    @abstractmethod
    def unsubscribe(self, observer):
        pass

    @abstractmethod
    def notify(self, data):
        pass


class ExchangeObserver(metaclass=ABCMeta):
    # data format: {full: True, 'asks': [[price, volume], ...], 'bids': [[price, volume], ...]}
    # full 是否全量更新
    @abstractmethod
    def update(self, data):
        pass


class Exchange(ExchangeBoost, metaclass=ABCMeta):
    @abstractmethod
    async def _run(self):
        pass

    def __init__(self):
        self.ping_sleep_time = 60
        self.observers = []
        # self.channels = set()
        # todo: 如何去重
        self.channels = []
        self.is_running = False
        self.ws_conn = None
        self.max_observers = 0

    def wipe_cache(self):
        for observer in self.observers:
            observer.update({})

    async def run(self):
        if self.is_running:
            return
        self.is_running = True
        if hasattr(self, '_ping'):
            asyncio.create_task(self.ping())
        while True:
            try:
                await self._run()
            except Exception as e:
                self.wipe_cache()
                logger.exception(e)

    async def ping(self):
        while True:
            await asyncio.sleep(self.ping_sleep_time)
            try:
                if self.ws_conn is None or self.ws_conn.closed:
                    continue
                await self._ping()
            except Exception as e:
                logger.exception(e)

    def subscribe(self, observer):
        if self.max_observers > 0 and len(self.observers) >= self.max_observers:
            raise RuntimeError(f"max observers limit {self.max_observers}")
        self.observers.append(observer)
        self.channels.append(observer.channel)

    def unsubscribe(self, observer):
        self.observers.remove(observer)
        self.channels = set([observer.channel for observer in self.observers])
