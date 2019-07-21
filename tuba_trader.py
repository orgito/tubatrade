#!/usr/bin/env python3

import argparse
import hashlib
import os
import time

import pandas as pd
import redis
import requests
from pymongo import MongoClient
from requests.packages.urllib3.exceptions import InsecureRequestWarning

SLACK_HOOK=os.getenv('SLACK_HOOK')


def chk_vol_spike(row, minutes, spike, hour, minvol=1000):
    ''' check vol spike '''
    if not row['quotes']:
        return False
    df = pd.DataFrame(row['quotes'])
    cut_hour = hour - pd.Timedelta(minutes=minutes*2)
    interval = str(minutes) + 'min'
    df['t'] = pd.to_datetime(df['t'], unit='s')
    df.set_index('t', inplace=True)
    df = df.resample(interval).agg({'o': 'first', 'h': 'max', 'l': 'min', 'c': 'last', 'v': 'sum'})
    df.dropna(how='any', inplace=True)
    df = df[df.index < hour]
    df = df.tail(2)
    if df.index[-1] < cut_hour:
        return False
    if df['v'][-2] < minvol:
        return False
    try:
        delta = (df['v'][-1] / df['v'][-2] - 1) * 100
    except IndexError:
        delta = 0
    if delta >= spike:
        last = df.index[-1] + pd.Timedelta(minutes=minutes)
        v1 = int(df['v'][-1])
        v2 = int(df['v'][-2])
        inc = df['c'][-1] - df['o'][-1]
        out = '<https://br.tradingview.com/chart/?symbol={}|`{:<7}`> `{} C: ${:05.2f} \u0394${:+#6.2f} \u0394V: {:#8.2f}% {} -> {}`\n'
        return out.format(row['_id'], row['_id'], last, df['c'][-1], inc, delta, v2, v1)
    else:
        return False


def chk_bulls_quee(row, candles, minutes, hour):
    ''' check bulls queue '''
    if not row['quotes']:
        return False
    df = pd.DataFrame(row['quotes'])
    interval = str(minutes) + 'min'
    df['t'] = pd.to_datetime(df['t'], unit='s')
    df.set_index('t', inplace=True)
    df = df.resample(interval).agg({'o': 'first', 'h': 'max', 'l': 'min', 'c': 'last', 'v': 'sum'})
    df.dropna(how='any', inplace=True)
    df = df[df.index < hour]
    df = df.tail(candles)
    bullish = len(df[df['o'] < df['c']])
    if bullish == candles:
        last = df.index[-1] + pd.Timedelta(minutes=minutes)
        inc = (df['c'][-1] / df['o'][0] - 1) * 100
        out = '<https://br.tradingview.com/chart/?symbol={}|`{:<7}`> `O: ${:05.2f} C: ${:05.2f} +{:.2f}% {} V: {}`\n'
        return out.format(row['_id'], row['_id'], df['o'][0], df['c'][-1], inc, last, int(df['v'][-1]))
    else:
        return False


def msg_sent(msg):
    ''' check if the message was already sent '''
    msgkey = 'TUBA.' + hashlib.sha256(msg.encode('utf-8')).hexdigest()
    rsrv = redis.Redis('localhost')
    if rsrv.get(msgkey):
        return True
    rsrv.setex(msgkey, True, 86400)
    return False


def main():
    ''' Disable invalid ssl certificate warning '''
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--candles", help="number of consecutive candles. default 4", type=int, default=4)
    parser.add_argument("-s", "--size", help="candle interval in minutes. default 60", type=int, choices=[1, 5, 10, 15, 30, 60], default=60)
    parser.add_argument("-k", "--spike", help="percentage spike in trade volume", type=int, default=0)
    args = parser.parse_args()

    candles = args.candles
    minutes = args.size
    interval = str(minutes) + 'min'
    spike = args.spike
    minvol = 1000

    url = SLACK_HOOK

    total = 0
    msg = ''

    client = MongoClient()
    stickers = client.stocks.stickers

    # Get the last round time
    hour = pd.to_datetime(round(time.time()/(minutes * 60)) *(minutes * 60) - 3600*3, unit='s')

    if spike:
        for row in stickers.find():
            mesg = chk_vol_spike(row, minutes, spike, hour, minvol)
            if mesg:
                total = total + 1
                msg += mesg
        msg = '*spike ALERT*\n*{}* stocks over *{}%* volume spike\nInterval: *{}*\n\n'.format(total, spike, interval) + msg
    else:
        for row in stickers.find():
            mesg = chk_bulls_quee(row, candles, minutes, hour)
            if mesg:
                total = total + 1
                msg += mesg
        msg = '*BULL QUEE ALERT*\n*{}* stocks with *{}* consecutive bulish candles\nInterval: *{}*\n\n'.format(total, candles, interval) + msg


    if total and not msg_sent(msg):
        payload = {'text': msg}
        requests.post(url, json=payload, verify=False)


if __name__ == "__main__":
    main()
