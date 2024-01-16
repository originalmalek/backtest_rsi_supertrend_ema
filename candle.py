import os
import json
import pandas as pd
import pandas_ta as ta
import telebot
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.exceptions import HTTPException
from pybit.unified_trading import HTTP

bybit_api_key = os.getenv('BYBIT_API_KEY')
bybit_api_secret = os.getenv('BYBIT_API_SECRET')

session = HTTP(
	testnet=False,
	api_key=bybit_api_key,
	api_secret=bybit_api_secret,
)
def get_klines(ticker, timeframe, start_time):
	klines_response = session.get_kline(
				category="inverse",
				symbol=ticker,
				interval=timeframe,
				start=start_time,

			)
	return klines_response ['result']['list']


def update_candles(ticker, timeframe):
	df = pd.read_csv(f'{ticker}_{timeframe}.csv')
	candle_quantity = 0
	while candle_quantity != 1:
		df = pd.read_csv(f'{ticker}_{timeframe}.csv')
		last_candle_time = df['Time'].iloc[0]
		df = df.iloc[1:]

		klines = get_klines(ticker, timeframe, last_candle_time)
		candle_quantity = len(klines)
		df = pd.concat([pd.DataFrame(klines, columns=df.columns), df], ignore_index=True)
		df.to_csv(f'{ticker}_{timeframe}.csv', index=False)


if __name__ == '__main__':
	ticker = 'BTCUSDT'
	timeframe = 720
	start_time = 1691574552

	klines = get_klines(ticker, timeframe, start_time)

	columns = ['Time', 'Open', 'High', 'Low', 'Close', 'Vol', 'Vol1']

	df = pd.DataFrame(klines, columns=columns)
	df.to_csv(f'{ticker}_{timeframe}.csv', index=False)

	update_candles(ticker, timeframe)
