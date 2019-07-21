#!/usr/bin/env python3
"""Detect volume spikes and notify via slack"""

import argparse
import datetime
import os

import pandas as pd
import urllib3

from quotes import get_quotes_days, get_symbols
from tubaslack import TubaSlack

SLACK_TOKEN = os.getenv('SLACK_TOKEN')


def historical_spike(quotes, period='15min'):
    """Detect historical spike"""
    spiked = []
    out = '{}<https://br.tradingview.com/chart/?symbol={}|`{:<7}`> `{:>+.2%} V: {:7} - Avg: {:7} {:5.1f} Stdv {:5.1f} Avgs`\n'
    msg = ''
    for row in quotes:
        # get stock symbol
        stock = row['_id']

        # get quotes data
        df = pd.DataFrame(row['quotes'])

        df = df.loc[:, ('t', 'c', 'o', 'v')]
        df['t'] = pd.to_datetime(df['t'], unit='s')
        df.set_index('t', inplace=True)

        dft = df.resample(period).agg({'o': 'first', 'c': 'last', 'v': 'sum'})
        dft = dft.loc[(dft.index.hour == 10) & (dft.index.minute == 0)]
        dft.dropna(inplace=True)

        if dft.empty:
            continue

        # skip if there is no data for today
        if dft.index[-1].date() < datetime.date.today():
            continue

        # get the 21 days avg and stdev
        avg = dft[-23:-2]['v'].mean()
        std = dft[-23:-2]['v'].std()
        lastv = dft.v.iat[-1]
        yestc = dft.c.iat[-2] # yesterday's close
        lastc = dft.c.iat[-1]
        avgs = round(lastv / avg, 2)
        stds = round((lastv - avg) / std, 2)
        var = lastc / yestc - 1
        direction = ':small_red_triangle:' if yestc < lastc else ':small_red_triangle_down:'
        if (lastv - avg) > (std * 2):
            spiked.append(stock)
            msg += out.format(direction, stock, stock, var, lastv, int(avg), stds, avgs)

    if spiked:
        return {'spikes': spiked, 'msg': msg}

    return None


def main():
    """ Check Volume Spikes """
    # disable get warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # get the parameters
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--size', help='Candle interval in minutes. Default 15', type=int, choices=[1, 2, 3, 5, 10, 15, 30, 60, 120], default=15)
    parser.add_argument('-t', '--test', help='Send message to #testes channel instead of #stockalerts', action='store_true')
    args = parser.parse_args()
    interval = f'{args.size}min'

    # slack settings
    slack = TubaSlack(token=SLACK_TOKEN)

    # get the stocks symbols from ibovespa
    symbols = get_symbols()

    # get 35 days back to make sure we have 21 trade days
    quotes = get_quotes_days(symbols, 35)

    res = historical_spike(quotes, interval)
    if res:
        msg = f':shark: *21 TRADE DAYS VOLUME SPIKES: 10h00 + {interval}*\n{res["msg"]}'
        if args.test:
            slack.api_call("chat.postMessage", channel="#testes", text=msg)
        else:
            slack.api_call("chat.postMessage", channel="#stockalerts", text=msg)


if __name__ == "__main__":
    main()
