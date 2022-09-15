import os
import sys
import json
import asyncio
import statistics
import aiohttp

BITGET_PERPS = 'https://api.bitget.com/api/mix/v1/market/contracts?productType=umcbl'
BINANCE_PERPS = 'https://fapi.binance.com/fapi/v1/exchangeInfo'
BINANCE_OI = 'https://fapi.binance.com/fapi/v1/openInterest'
BYBIT_PERPS = 'https://api.bybit.com/v2/public/tickers'
FTX_PERPS = 'https://ftx.com/api/futures'
ABSOLUTE_PATH = os.path.dirname(os.path.abspath(__file__))
with open(os.path.join(ABSOLUTE_PATH, 'token.txt'), 'r') as file:
    TOKEN = file.readline()


def aggregate_oi(responses):
    """
    Outermost dict contains binance coins with their open interest, ftx and bybit keys contain list of dicts {coin: OI}
    :dict responses: {'coin1': oi, 'ftx':[{'coin1': oi}, 'bybit':[{'coin1': oi}}
    :return: {coin1: binance, ftx, bybit perp open interest per coin,}
    """
    curr_oi = {}
    for r in responses:
        if not r:
            continue
        key1 = list(r.keys())[0]
        if key1 in ('bybit', 'ftx'):
            for sym in r[key1]:
                key2 = list(sym.keys())[0]
                curr_oi[key2] = curr_oi.get(key2, 0) + sym[key2]
        else:
            curr_oi[key1] = curr_oi.get(key1, 0) + r[key1]

    return curr_oi


async def bitget_perps(session):
    """
    coins open interest watchlist from bitget usdt perps
    :param session: ClientSession
    """
    bitget_base_coins = set()
    async with session.get(BITGET_PERPS) as resp:
        resp = await resp.json()

    for perp in resp['data']:
        if perp['quoteCoin'] == 'USDT':
            bitget_base_coins.add(perp['baseCoin'])

    return bitget_base_coins


async def get_oi_binance(session, symbol, coin):
    """
    Binance USDT perps open interest of coins in watchlist
    :param session: ClientSession
    :param symbol: Binance USDT perp symbol to look up
    :param coin: Binance USDT perp base coin
    :return: [{coin: open interest}, None,]
    """
    async with session.get(BINANCE_OI, params={'symbol': symbol}) as resp:
        stat = resp.status
        resp = await resp.json()
        if stat == 200:
            return {coin: float(resp['openInterest'])}
        elif stat == 429:
            print(stat, resp)
            sys.exit()
        else:
            print(stat, resp)


async def get_oi_bybit(session, bitget_base_coins):
    """
    Bybit USDT perps open interest of coins in watchlist
    :param session: ClientSession
    :param bitget_base_coins: set() coins watchlist
    :return: {bybit: [{coin: open interest}, ]}
    """
    bybit_oi = []
    async with session.get(BYBIT_PERPS) as resp:
        resp = await resp.json()

    for perp in resp['result']:
        coin = perp['symbol'].replace('USDT', '')
        if 'USDT' in perp['symbol'] and coin in bitget_base_coins:
            bybit_oi.append({coin: float(perp['open_interest'])})

    return {'bybit': bybit_oi}


async def get_oi_ftx(session, bitget_base_coins):
    """
    FTX USDT perps open interest of coins in watchlist
    :param session: ClientSession
    :param bitget_base_coins: set() coins watchlist
    :return: {ftx: [{coin: open interest},]}
    """
    ftx_oi = []
    async with session.get(FTX_PERPS) as resp:
        resp = await resp.json()

    for perp in resp['result']:
        coin = perp['underlying']
        if perp['perpetual'] and coin in bitget_base_coins:
            ftx_oi.append({coin: float(perp['openInterest'])})

    return {'ftx': ftx_oi}


async def oi(session, bitget_base_coins):
    """
    Get open interest of coins in watchlist from Binance, Bybit and FTX
    :param session: ClientSession
    :param bitget_base_coins: set() coins watchlist
    :return: {coin1: binance, ftx, bybit perp open interest per coin,}
    """
    tasks = []
    async with session.get(BINANCE_PERPS) as resp:
        resp = await resp.json()

    for perp in resp['symbols']:
        if perp['contractType'] == 'PERPETUAL' \
                and perp['quoteAsset'] == 'USDT' and perp['baseAsset'] in bitget_base_coins:
            tasks.append(asyncio.create_task(get_oi_binance(session, perp['symbol'], perp['baseAsset'])))

    tasks.append(asyncio.create_task(get_oi_bybit(session, bitget_base_coins)))
    tasks.append(asyncio.create_task(get_oi_ftx(session, bitget_base_coins)))
    responses = await asyncio.gather(*tasks, return_exceptions=True)

    return aggregate_oi(responses)


async def send_tg(session, curr_oi):
    """
    Send telegram message of coins OI percentage changes
    :param session: ClientSession
    :param curr_oi: {coin: aggregated oi,}
    """
    file_path = os.path.join(ABSOLUTE_PATH, 'agg_oi.json')

    with open(file_path, 'r') as json_file:
        prev_oi = json.load(json_file)

    with open(file_path, 'w') as outfile:
        json.dump(curr_oi, outfile)

    oi_diff = {}
    for coin in curr_oi.keys():
        if curr_oi[coin] - prev_oi[coin] == 0:
            oi_diff[coin] = 0
        else:
            oi_diff[coin] = (curr_oi[coin] - prev_oi[coin]) / prev_oi[coin]

    oi_change_mean = statistics.mean(list(oi_diff.values()))
    oi_std_dev = statistics.stdev(list(oi_diff.values()))

    top_dogs = ''
    gainers = ''
    losers = ''
    for coin in oi_diff:
        if coin in ('ETH', 'BTC'):
            top_dogs += f'{coin} : {oi_diff[coin]:.2%}%0A'
        elif oi_diff[coin] >= oi_change_mean + oi_std_dev:
            gainers += f'{coin} : {oi_diff[coin]:.2%}%0A'
        elif oi_diff[coin] <= oi_change_mean - oi_std_dev:
            losers += f'{coin} : {oi_diff[coin]:.2%}%0A'

    url = f'https://api.telegram.org/bot{TOKEN}/getUpdates'
    async with session.get(url) as resp:
        resp = await resp.json()
    chat_id = resp['result'][0]['message']['chat']['id']
    msg = '*-------------------*%0A' + top_dogs + '%0A' + gainers + '%0A' + losers + '*-------------------*'
    msg = msg.replace('-', '\\-').replace('.', '\\.')
    url = f'https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={chat_id}&text={msg}&parse_mode=MarkdownV2'
    async with session.get(url) as resp:  # send msg
        print(await resp.json())


async def main():
    async with aiohttp.ClientSession() as session:
        bitget_base_coins = await bitget_perps(session)
        curr_oi = await oi(session, bitget_base_coins)
        await send_tg(session, curr_oi)


if __name__ == '__main__':
    asyncio.run(main())
