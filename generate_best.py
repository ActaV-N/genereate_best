import time
from pytz import timezone
import sqlite3
import datetime
import pyupbit
import requests
import numpy as np

myToken = 'xoxb-3522240960336-3495597730933-Wl4ediBpKdAarCUbgEdctUYk'

def post_message( text, channel='#auto-trading', token=myToken):
    """슬랙 메시지 전송"""
    response = requests.post("https://slack.com/api/chat.postMessage",
        headers={"Authorization": "Bearer "+token},
        data={"channel": channel,"text": text}
    )

def get_start_time(ticker):
    """시작 시간 조회"""
    df = pyupbit.get_ohlcv(ticker, interval="day", count=1)
    start_time = df.index[0]
    return start_time

def get_ror(df, strategy='normal', k=0.5):
    df = df.copy()
    df['range'] = (df['high'] - df['low']) * k
    df['target'] = df['open'] + df['range'].shift(1)
    
    df['short_ema'] = df['close'].ewm(span=12).mean()
    df['long_ema'] = df['close'].ewm(span=26).mean()
    
    df['macd'] = df['short_ema'] - df['long_ema']
    df['signal'] = df['macd'].ewm(span=9).mean()
    
    macd_bull = df['macd'] > df['signal']
    
    df['bull'] = True
    
    if strategy=='macd':
        df['bull'] = macd_bull

    
    fee = 0.0032
    
    df['ror'] = np.where((df['close'] > df['target']) & df['bull'], df['close'] / df['target'] - fee, 1)
    df['hpr'] = df['ror'].cumprod()
    df['dd'] = (df['hpr'].cummax() - df['hpr']) / df['hpr'].cummax() * 100
    df['mdd'] = df['dd'].max()

    return (df, df['hpr'][-2])

def get_best_k(df, strategy='normal'):
    best_k = 0.1
    c_ror = 0
    for i in np.arange(0.1, 1.0, 0.1):
        (_,ror) = get_ror(df, strategy=strategy, k=i)
        
        if(ror > c_ror):
            best_k = i
            c_ror = ror
    return best_k

def get_best(strategy):
    hprs = []
    tickers = pyupbit.get_tickers('KRW')
    
    for i in range(len(tickers)):
        ticker = tickers[i]
        data = pyupbit.get_ohlcv(ticker, count=26, interval='day')
        if data is not None:
            k = get_best_k(data, strategy = strategy)
            (df, hpr) = get_ror(data, strategy=strategy, k=k)
            hprs.append((ticker, hpr, k))

    sorted_hpr = sorted(hprs, key=lambda x:x[1], reverse=True)
    return sorted_hpr[0]

conn = sqlite3.connect('./best_ticker_20.db')
cur = conn.cursor()

cur.execute('''CREATE TABLE if not exists best_20(
                id INTEGER PRIMARY KEY autoincrement,
                normal_ticker TEXT,
                normal_k FLOAT,
                macd_ticker TEXT,
                macd_k FLOAT,
                date TEXT
)''')
row = cur.execute('SELECT * FROM best_20')

now = datetime.datetime.now(timezone('Asia/Seoul'))
current_date = now.strftime('%Y-%m-%d')
if row.fetchone() is None:
    (normal_ticker, normal_hpr, normal_k) = get_best('normal')
    (macd_ticker, macd_hpr, macd_k) = get_best('macd')
    
    cur.execute('''
        INSERT INTO best_20(normal_ticker, normal_k, macd_ticker, macd_k, date) VALUES(?, ?, ?, ?, ?)
    ''', [normal_ticker, normal_k, macd_ticker, macd_k, current_date])

    conn.commit()
    conn.close()

    print("Normal Ticker: %s, Normal K: %.1f, Normal HPR: %.2f" % (normal_ticker, normal_k, normal_hpr), flush=True)
    post_message("Normal Ticker: %s, Normal K: %.1f, Normal HPR: %.2f" % (normal_ticker, normal_k, normal_hpr))
    print("MACD Ticker: %s, MACD K: %.1f, MACD HPR: %.2f" % (macd_ticker, macd_k, macd_hpr), flush=True)
    post_message("MACD Ticker: %s, MACD K: %.1f, MACD HPR: %.2f" % (macd_ticker, macd_k, macd_hpr))


load_flag = False

while True:
    try:
        now = datetime.datetime.now(timezone('Asia/Seoul'))
        
        start_time = get_start_time("KRW-BTC")
        end_time = start_time + datetime.timedelta(days=1)

        renewal_time = end_time - datetime.timedelta(minutes=30)

        if renewal_time < now < end_time and not load_flag:
            current_date = now.strftime('%Y-%m-%d')
            
            conn = sqlite3.connect('./best_ticker_20.db')
            cur = conn.cursor()

            (normal_ticker, normal_hpr, normal_k) = get_best('normal')
            (macd_ticker, macd_hpr, macd_k) = get_best('macd')
            
            cur.execute('''
                INSERT INTO best_20(normal_ticker, normal_k, macd_ticker, macd_k, date) VALUES(?, ?, ?, ?, ?)
            ''', [normal_ticker, normal_k, macd_ticker, macd_k, current_date])

            conn.commit()
            conn.close()
            print("Normal Ticker: %s, Normal K: %.1f, Normal HPR: %.2f" % (normal_ticker, normal_k, normal_hpr), flush=True)
            post_message("Normal Ticker: %s, Normal K: %.1f, Normal HPR: %.2f" % (normal_ticker, normal_k, normal_hpr))
            print("MACD Ticker: %s, MACD K: %.1f, MACD HPR: %.2f" % (macd_ticker, macd_k, macd_hpr), flush=True)
            post_message("MACD Ticker: %s, MACD K: %.1f, MACD HPR: %.2f" % (macd_ticker, macd_k, macd_hpr))
            
            load_flag = True

        if now > end_time:
            load_flag = False

        time.sleep(1)
    except Exception as e:
        print(e, flush=True)
        time.sleep(1)
