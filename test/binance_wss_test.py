import ccxt.async_support as asyncccxt
import ccxtws
import time
import json
import asyncio


# tickers
def ws_tickers(ws_api):
    tickers = {}
    def _callback(rsp):
        # print_beauty_json(rsp)
        tickers.update(rsp)
        print(tickers)

    observer = ccxtws.binance_observer("tickers", {}, _callback)
    ws_api.subscribe(observer)
    asyncio.get_event_loop().run_until_complete(ws_api.run())

# order book
def ws_order_book(ws_api):
    ob = {}
    def _callback(rsp):
        # print_beauty_json(rsp)
        ob.update(rsp)
        print(ob)

    symbol = "BTC/USDT"
    observer = ccxtws.binance_observer("order_book", {"symbol": symbol, "levels": 5}, _callback)
    ws_api.subscribe(observer)
    asyncio.get_event_loop().run_until_complete(ws_api.run())

# # order book
# async def ws_order_book(ws_api):
#     ob = {}
#     def _callback(rsp):
#         # print_beauty_json(rsp)
#         ob.update(rsp)
#         print(ob)
#
#     symbol = "BTC/USDT"
#     observer = ccxtws.binance_observer("order_book", {"symbol": symbol, "levels": 5}, _callback)
#     ws_api.subscribe(observer)
#     await asyncio.gather(asyncio.ensure_future(ws_api.run()))

# # trade
# def ws_trade(ws_api):
#     trades = {}
#     def _callback(rsp):
#         # print_beauty_json(rsp)
#         if len(rsp) > 0:
#             # trades.append(rsp)
#             trades.update(rsp)
#             print(trades)
#     symbol = "BTC/USDT"
#     observer = ccxtws.binance_observer("trade", {"symbol": symbol}, _callback)
#     ws_api.subscribe(observer)
#     asyncio.get_event_loop().run_until_complete(ws_api.run())

# trade
async def ws_trade(ws_api):
    trades = {}
    def _callback(rsp):
        # print_beauty_json(rsp)
        if len(rsp) > 0:
            # trades.append(rsp)
            trades.update(rsp)
            print(trades)

    await ws_api.exchange.load_markets(params={"type": "future"})
    symbol = "BTC/USDT"
    observer = ccxtws.binance_observer("trade", {"symbol": symbol}, _callback)
    ws_api.subscribe(observer)
    await asyncio.gather(asyncio.ensure_future(ws_api.run()))


if __name__ == "__main__":
    ws_api = ccxtws.binance("future_u", {})
    asyncio.get_event_loop().run_until_complete(ws_trade(ws_api))