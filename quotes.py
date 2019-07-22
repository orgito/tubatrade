"""Quote utils"""

import asyncio
import os
from time import sleep, time

import aiohttp
import async_timeout
import nest_asyncio
import requests
from lxml import html

QUOTES_DATA_URL=os.getenv('QUOTES_DATA_URL')

async def _fetch(session, symbol, them, now, resolution='1'):
    """Fetch stock data for one sumbol"""
    trader_url = QUOTES_DATA_URL+'?symbol={}&resolution={}&from={}&to={}'

    url = trader_url.format(symbol, resolution, them, now)
    with async_timeout.timeout(60):
        async with session.get(url) as response:
            result = {'_id': symbol, 'quotes': await response.json()}
            return result


async def _get_quotes(symbols, them, now, resolution='1'):
    """Fetch stock data for all symbols"""
    conn = aiohttp.TCPConnector(ssl=False)
    async with aiohttp.ClientSession(connector=conn, trust_env=True) as session:
        tasks = [_fetch(session, symbol, them, now, resolution) for symbol in symbols]
        responses = await asyncio.gather(*tasks)
        return responses


def get_quotes(symbols, them=None, now=None, resolution='1'):
    """Fetch stock data for all symbols"""
    if now is None:
        now = round(time())
    if them is None:
        them = now - 24 * 3600
    # TODO: Understand why this is necessary
    nest_asyncio.apply()
    loop = asyncio.get_event_loop()
    quotes = loop.run_until_complete(_get_quotes(symbols, them, now, resolution))
    return quotes


def get_quotes_days(symbols, days=1, resolution='1'):
    """Fetch stock data for all symbols for the number of days specified"""
    now = round(time())
    them = now - 24 * 3600 * days
    return get_quotes(symbols, them, now, resolution)


def get_quotes_hours(symbols, hours=12, resolution='1'):
    """Fetch stock data for all symbols for the number of hours specified"""
    now = round(time())
    them = now - 3600 * hours
    return get_quotes(symbols, them, now, resolution)


def get_symbols(indice='IBOV'):
    """ Get the list of symbols for the specified Bovespa índice """
    url = f'http://bvmf.bmfbovespa.com.br/indices/ResumoCarteiraTeorica.aspx?Indice={indice}&idioma=pt-br'
    page = requests.get(url)

    # horrible hack
    i = 0
    while len(page.text) < 100000 and i < 5:
        sleep(0.1)
        page = requests.get(url)
        i += 1
    if len(page.text) < 100000:
        raise RuntimeError('Unable to get symbols')

    tree = html.fromstring(page.content)
    symbols = tree.xpath(('//tr[contains(@class, "ItemStyle") and contains(@class, "GridBovespa")]/td[@class="rgSorted"]'
                          '/span[contains(@id, "_contentPlaceHolderConteudo_grdResumoCarteiraTeorica_")]/text()'))
    return symbols
