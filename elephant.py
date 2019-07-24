#!/usr/bin/env python3
"""Detect elephant candles and notify via slack"""

import argparse
import datetime
import os

import pandas as pd
import urllib3

from quotes import get_quotes_days, get_symbols
from tubaslack import TubaSlack

SLACK_TOKEN = os.getenv('SLACK_TOKEN')


def elephants(quotes, period='5min', times=1.5):
    """Detect elephant candles"""
    elephant = []
    out = '<https://br.tradingview.com/chart/?symbol={}|`{:<7}`> `{:5.2f}`\n'
    msg = ''
    for row in quotes:
        # get stock symbol
        stock = row['_id']

        # get quotes data
        df = pd.DataFrame(row['quotes'])

        df = df.loc[:, ('t', 'c', 'o')]
        df['t'] = pd.to_datetime(df['t'], unit='s')
        df.set_index('t', inplace=True)

        df = df.resample(period).agg({'o': 'first', 'c': 'last'})
        df.dropna(inplace=True)
        df['size'] = abs(df['c'] - df['o'])
        tail = list(df['size'][-6:])

        if tail[-1] > times*max(tail[:-1]):
            elephant.append(stock)
            msg += out.format(stock, stock, df['c'][-1])

    if elephant:
        return {'Elephants': elephant, 'msg': msg}

    return None


def main():
    """ Check Volume Spikes """
    # disable get warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # get the parameters
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--size', help='Candle interval in minutes. Default 5', type=int, choices=[5, 10, 15, 30, 60], default=5)
    parser.add_argument('-x', '--times', help='How bigger should the candle be. Default 1.5', type=float, default=1.5)
    parser.add_argument('-t', '--test', help='Send message to #testes channel instead of #stockalerts', action='store_true')
    args = parser.parse_args()
    interval = f'{args.size}min'
    times = args.times

    # slack settings
    slack = TubaSlack(token=SLACK_TOKEN)

    # get the stocks symbols from ibovespa
    symbols = get_symbols()

    quotes = get_quotes_days(symbols, 4)

    res = elephants(quotes, interval, times)
    if res:
        msg = f':elephant::candle::candle: *ELEPHANT CANDLES: {interval}*\n{res["msg"]}'
        if args.test:
            slack.api_call("chat.postMessage", channel="#testes", text=msg)
        else:
            slack.api_call("chat.postMessage", channel="#stockalerts", text=msg)


if __name__ == "__main__":
    main()
