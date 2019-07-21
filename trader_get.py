#!/usr/bin/env python3

import os
import asyncio
from time import time

from urllib3.exceptions import InsecureRequestWarning
from urllib3 import disable_warnings

import pandas as pd

from pymongo import MongoClient

import aiohttp
import async_timeout

QUOTES_DATA_URL=os.getenv('QUOTES_DATA_URL')
QUOTES_DATA_TOKEN=os.getenv('QUOTES_DATA_TOKEN')


IBOVESPA = ('ABEV3', 'PETR4', 'ITSA4', 'VALE3', 'ITUB4', 'BBDC4', 'PETR3', 'BVMF3', 'JBSS3', 'KROT3', 'BBAS3', 'CIEL3', 'CCRO3', 'GGBR4',
            'BRML3', 'CMIG4', 'TIMP3', 'BRFS3', 'RAIL3', 'EMBR3', 'LREN3', 'BBSE3', 'LAME4', 'CSNA3', 'GOAU4', 'BBDC3', 'WEGE3', 'USIM5',
            'CPFE3', 'KLBN11', 'VIVT4', 'HYPE3', 'MRFG3', 'UGPA3', 'SUZB5', 'SANB11', 'SBSP3', 'ESTC3', 'MRVE3', 'BRKM5', 'ELET3', 'CYRE3',
            'ENBR3', 'QUAL3', 'FIBR3', 'ELET6', 'BRAP4', 'EGIE3', 'RADL3', 'ECOR3', 'EQTL3', 'NATU3', 'RENT3', 'PCAR4', 'CSAN3', 'CPLE6',
            'MULT3', 'SMLE3', 'IBOV')

IBRX100 = ('ABEV3', 'PETR4', 'ITSA4', 'VALE3', 'ITUB4', 'BBDC4', 'PETR3', 'BVMF3', 'JBSS3', 'KROT3', 'BBAS3', 'CIEL3', 'CCRO3', 'GGBR4',
           'BRML3', 'CMIG4', 'TIMP3', 'BBDC3', 'BRFS3', 'RAIL3', 'EMBR3', 'LREN3', 'BBSE3', 'LAME4', 'CSNA3', 'GOAU4', 'WEGE3', 'POMO4',
           'UGPA3', 'USIM5', 'CPFE3', 'KLBN11', 'VIVT4', 'HYPE3', 'MRFG3', 'SUZB5', 'SANB11', 'SBSP3', 'SAPR4', 'FLRY3', 'ESTC3', 'TIET11',
           'MRVE3', 'SULA11', 'DTEX3', 'BRKM5', 'ELET3', 'ODPV3', 'CYRE3', 'ENBR3', 'QUAL3', 'FIBR3', 'ELET6', 'BRAP4', 'ENGI11', 'EGIE3',
           'RADL3', 'ECOR3', 'EQTL3', 'LAME3', 'TAEE11', 'CESP6', 'RAPT4', 'BRSR6', 'NATU3', 'BEEF3', 'BTOW3', 'RENT3', 'SMTO3', 'PCAR4',
           'ALUP11', 'CSAN3', 'TOTS3', 'GOLL4', 'VVAR11', 'MYPK3', 'HGTX3', 'BRPR3', 'ELPL4', 'CVCB3', 'ALSC3', 'CPLE6', 'TRPL4', 'LIGT3',
           'PSSA3', 'MDIA3', 'IGTA3', 'GRND3', 'MULT3', 'QGEP3', 'VLID3', 'TUPY3', 'CSMG3', 'EZTC3', 'SMLE3', 'MPLU3', 'SEER3', 'GFSA3',
           'MGLU3', 'IBOV')


TRADER_URL = QUOTES_DATA_URL+'?symbol={}&resolution={}&from={}&to={}'
HEADERS = {'x-access-token': QUOTES_DATA_TOKEN}


# Get all quotes
async def fetch(session, symbol, them, now, resolution='1'):
    url = TRADER_URL.format(symbol, resolution, them, now)
    with async_timeout.timeout(180):
        async with session.get(url, headers=HEADERS) as response:
            try:
                # print('OK ', symbol, response.status)
                result = {'_id': symbol, 'quotes': await response.json()}
            except:
                # print('ERR', symbol, response.status)
                result = {'_id': symbol, 'quotes': False}
            return result


async def get_quotes(symbols, them, now, resolution='1'):
    conn = aiohttp.TCPConnector(verify_ssl=False)
    async with aiohttp.ClientSession(connector=conn) as session:
        tasks = [fetch(session, symbol, them, now, resolution) for symbol in symbols]
        responses = await asyncio.gather(*tasks)
        return responses


def main():
    client = MongoClient()
    stickers = client.stocks.stickers

    symbols = IBRX100

    now = round(time())
    them = now - 3600

    loop = asyncio.get_event_loop()
    responses = loop.run_until_complete(get_quotes(symbols, them, now, '1'))

    # responses = []
    # print('---20----')
    # responses.extend(loop.run_until_complete(get_quotes(symbols[:20], them, now, '1')))
    # print('---40----')
    # responses.extend(loop.run_until_complete(get_quotes(symbols[20:40], them, now, '1')))
    # print('---60----')
    # responses.extend(loop.run_until_complete(get_quotes(symbols[40:60], them, now, '1')))
    # print('---80----')
    # responses.extend(loop.run_until_complete(get_quotes(symbols[60:80], them, now, '1')))
    # print('---100----')
    # responses.extend(loop.run_until_complete(get_quotes(symbols[80:], them, now, '1')))
    # print('----------')

    for response in responses:
        if not response['quotes']:
            print('Skipping', response['_id'])
            continue
        df = pd.DataFrame(response['quotes'])
        df.drop('s', axis=1, inplace=True)
        df = df[['t', 'o', 'h', 'l', 'c', 'v']]
        df = df[:-1]
        quotes = df.to_dict(orient='records')
        if quotes:
            stickers.find_one_and_update({"_id": response['_id']}, {"$addToSet": {"quotes": {"$each": quotes}}}, upsert=True)


if __name__ == "__main__":
    main()
