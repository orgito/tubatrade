#!/usr/bin/env python3
"""Detect spike at the end of day"""

import argparse
import datetime
import os

import pandas as pd
import urllib3

from quotes import get_quotes_hours, get_symbols
from tubaslack import TubaSlack

SLACK_TOKEN = os.getenv('SLACK_TOKEN')


def end_of_day_spike(quotes, period='3min'):
    """Detect end of day spikes"""
    spiked = []
    out = '{}<https://br.tradingview.com/chart/?symbol={}|`{:<7}`> `V: {:7} - Size: {:5.1f}`\n'
    msg = ''
    for row in quotes:
        # get stock symbol
        stock = row['_id']

        # get quotes data
        df = pd.DataFrame(row['quotes'])
        df.drop(columns=['h', 'l', 's'], inplace=True)

    return {'msg': msg}


def main():
    """ Check end of day spikes """
    # disable get warnings
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    # get the parameters
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--test', help='Send message to #testes channel instead of #stockalerts', action='store_true')
    args = parser.parse_args()

    # slack settings
    slack = TubaSlack(token=SLACK_TOKEN)

    symbols = get_symbols()
    quotes = get_quotes_hours(symbols, 10)

    res = end_of_day_spike(quotes)

    if res:
        msg = f':shark: *END OF DAY SPIKES*\n{res["msg"]}'
        if args.test:
            slack.api_call("chat.postMessage", channel="#testes", text=msg)
        else:
            slack.api_call("chat.postMessage", channel="#stockalerts", text=msg)
