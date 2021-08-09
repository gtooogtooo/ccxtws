import asyncio
import json
import time
import websockets
import ccxt.async_support as asyncccxt
from ccxt.base.exchange import Exchange as BaseExchange
from ccxt.base.precise import Precise
from ccxtws.base import Exchange, ExchangeObserver, logger
from . import utils


class binance(Exchange):
    future_u_url = "wss://fstream.binance.com/ws/stream"
    future_base_url = "wss://dstream.binance.com/ws/stream"
    spot_url = "wss://stream.binance.com:9443/ws/stream"

    TRADE = "{symbol}@aggTrade"
    ORDER_BOOK = "{symbol}@depth{levels}@100ms"
    TICKERS = "!ticker@arr"

    def __init__(self, ws_type='spot', cfg={}):
        super().__init__()
        # https://binance-docs.github.io/apidocs/spot/cn/#6ae7c2b506
        self.ws_uri = getattr(self, f"{ws_type}_url")
        self.max_observers = 1024
        self.exchange = asyncccxt.binance(cfg)
        # while 1:
        #     try:
        #         asyncio.get_event_loop().run_until_complete(self.exchange.load_markets())
        #         break
        #     except:
        #         pass

    async def _run(self):
        await self.exchange.load_markets()
        async with websockets.connect(self.ws_uri) as websocket:
            self.ws_conn = websocket
            req = json.dumps({"method": "SET_PROPERTY", "params": ["combined", True], "id": utils.get_req_id()})
            await websocket.send(req)

            added_channels = set()
            while True:
                for channel in self.channels:
                    stream = channel["stream"]
                    if stream is None:
                        params = channel["params"]
                        feed_type = channel["feed_type"]
                        symbol = params.get("symbol")
                        if symbol is not None:
                            market_id = self.exchange.markets[symbol]["lowercaseId"]
                            params["symbol"] = market_id
                        stream = getattr(self, f"get_{feed_type}_stream")(params)
                        channel["stream"] = stream
                    if stream in added_channels:
                        continue
                    added_channels.add(stream)
                    req = json.dumps({"method": "SUBSCRIBE", "params": [stream], "id": utils.get_req_id()})
                    await websocket.send(req)
                    await asyncio.sleep(0.25)
                resp = await websocket.recv()
                data = json.loads(resp)
                if 'ping' in data:
                    req = json.dumps({"pong": data['ping']})
                    await websocket.send(req)
                else:
                    self.notify(data)

    def notify(self, data):
        if 'data' not in data:
            logger.warning("unknown data %s", data)
            return

        # final_data = {'full': True, 'asks': [], 'bids': []}
        # final_data['asks'] = [[float(item[0]), float(item[1])] for item in data['data']['asks']]
        # final_data['bids'] = [[float(item[0]), float(item[1])] for item in data['data']['bids']]

        final_data = None
        for observer in self.observers:
            channel = observer.channel
            if channel["stream"] not in data['stream']:
                continue
            if final_data is None:
                final_data = getattr(self, f"parse_{channel['feed_type']}")(data, channel["params"])
            observer.update(final_data)

    def parse_ticker(self, ticker, market=None):
        """
        {
          "e": "24hrTicker",  // 事件类型
          "E": 123456789,     // 事件时间
          "s": "BNBBTC",      // 交易对
          "p": "0.0015",      // 24小时价格变化
          "P": "250.00",      // 24小时价格变化(百分比)
          "w": "0.0018",      // 平均价格
          "x": "0.0009",      // 整整24小时之前，向前数的最后一次成交价格
          "c": "0.0025",      // 最新成交价格
          "Q": "10",          // 最新成交交易的成交量
          "b": "0.0024",      // 目前最高买单价
          "B": "10",          // 目前最高买单价的挂单量
          "a": "0.0026",      // 目前最低卖单价
          "A": "100",         // 目前最低卖单价的挂单量
          "o": "0.0010",      // 整整24小时前，向后数的第一次成交价格
          "h": "0.0025",      // 24小时内最高成交价
          "l": "0.0010",      // 24小时内最低成交价
          "v": "10000",       // 24小时内成交量
          "q": "18",          // 24小时内成交额
          "O": 0,             // 统计开始时间
          "C": 86400000,      // 统计结束时间
          "F": 0,             // 24小时内第一笔成交交易ID
          "L": 18150,         // 24小时内最后一笔成交交易ID
          "n": 18151          // 24小时内成交数
        }
        """
        self = self.exchange
        timestamp = self.safe_integer(ticker, 'C')
        marketId = self.safe_string(ticker, 's')
        symbol = self.safe_symbol(marketId, market)
        last = self.safe_number(ticker, 'c')
        baseVolume = self.safe_number(ticker, 'v')
        quoteVolume = self.safe_number(ticker, 'q')
        return {
            'symbol': symbol,
            'timestamp': timestamp,
            'datetime': self.iso8601(timestamp),
            'high': self.safe_number(ticker, 'h'),
            'low': self.safe_number(ticker, 'l'),
            'bid': self.safe_number(ticker, 'b'),
            'bidVolume': self.safe_number(ticker, 'B'),
            'ask': self.safe_number(ticker, 'a'),
            'askVolume': self.safe_number(ticker, 'A'),
            'vwap': self.safe_number(ticker, 'w'),
            'open': self.safe_number(ticker, 'o'),
            'close': last,
            'last': last,
            'previousClose': None,  # previous day close
            'change': self.safe_number(ticker, 'p'),
            'percentage': self.safe_number(ticker, 'P'),
            'average': None,
            'baseVolume': baseVolume,
            'quoteVolume': quoteVolume,
            # 'info': ticker,
        }

    def parse_tickers(self, data, params={}):
        data = data['data']
        self.exchange.parse_ticker = self.parse_ticker
        tickers = self.exchange.parse_tickers(data)
        return tickers

    def parse_order_book(self, data, params):
        data = data['data']
        self = self.exchange
        timestamp = int(time.time()*1000)
        symbol = self.markets_by_id[str(params['symbol']).upper()]["symbol"]
        orderbook = self.parse_order_book(data, symbol, timestamp)
        orderbook['nonce'] = self.safe_integer(data, 'lastUpdateId')
        return orderbook

    def parse_trade(self, data, params):
        trade = data["data"]
        # trade = data
        self = self.exchange
        # market = self.market(params['symbol'])
        '''
        {
          "e": "aggTrade",  // 事件类型
          "E": 123456789,   // 事件时间
          "s": "BNBBTC",    // 交易对
          "a": 12345,       // 归集交易ID
          "p": "0.001",     // 成交价格
          "q": "100",       // 成交笔数
          "f": 100,         // 被归集的首个交易ID
          "l": 105,         // 被归集的末次交易ID
          "T": 123456785,   // 成交时间
          "m": true,        // 买方是否是做市方。如true，则此次成交是一个主动卖出单，否则是一个主动买入单。
          "M": true         // 请忽略该字段
        }
        '''
        timestamp = self.safe_integer(trade, 'T')
        priceString = self.safe_string(trade, 'p')
        amountString = self.safe_string(trade, 'q')
        price = self.parse_number(priceString)
        amount = self.parse_number(amountString)
        symbol = self.markets_by_id[self.safe_string(trade, 's')]["symbol"]
        costString = Precise.string_mul(priceString, amountString)
        cost = self.parse_number(costString)
        id = self.safe_string(trade, 'a')
        side = None
        # orderId = self.safe_string(trade, 'orderId')
        if 'm' in trade:
            side = 'sell' if trade['m'] else 'buy'  # self is reversed intentionally
        elif 'isBuyerMaker' in trade:
            side = 'sell' if trade['isBuyerMaker'] else 'buy'
        elif 'side' in trade:
            side = self.safe_string_lower(trade, 'side')
        else:
            if 'isBuyer' in trade:
                side = 'buy' if trade['isBuyer'] else 'sell'  # self is a True side
        # fee = None
        # if 'commission' in trade:
        #     fee = {
        #         'cost': self.safe_number(trade, 'commission'),
        #         'currency': self.safe_currency_code(self.safe_string(trade, 'commissionAsset')),
        #     }
        # takerOrMaker = None
        # if 'isMaker' in trade:
        #     takerOrMaker = 'maker' if trade['isMaker'] else 'taker'
        # if 'maker' in trade:
        #     takerOrMaker = 'maker' if trade['maker'] else 'taker'
        return {
            # 'info': trade,
            'timestamp': timestamp,
            'datetime': self.iso8601(timestamp),
            'symbol': symbol,
            'id': id,
            # 'order': orderId,
            # 'type': None,
            'side': side,
            # 'takerOrMaker': takerOrMaker,
            'price': price,
            'amount': amount,
            'cost': cost,
            # 'fee': fee,
        }

    @staticmethod
    def get_trade_stream(params):
        return BaseExchange.implode_params(binance.TRADE, {
            "symbol": params["symbol"]
        })

    @staticmethod
    def get_order_book_stream(params):
        return BaseExchange.implode_params(binance.ORDER_BOOK, {
            "symbol": params["symbol"],
            "levels": params["levels"]
        })

    @staticmethod
    def get_tickers_stream(params={}):
        return binance.TICKERS


class binance_observer(ExchangeObserver):

    def __init__(self, feed_type, params, callback):
        self.channel = dict(
            feed_type=feed_type,
            params=params,
            stream=None
        )
        self.callback = callback

    def update(self, data):
        self.callback(data)


