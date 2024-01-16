from datetime import datetime

import pandas as pd
import pandas_ta as ta

pd.set_option('display.max_columns', None)


def create_signal_df(df):
	supertrend_df = ta.supertrend(high=df['High'],
	                              low=df['Low'],
	                              close=df['Close'],
	                              length=supertrend_low_length,
	                              multiplier=supertrend_low_multiplier
	                              )
	high_supertrend_df = ta.supertrend(high=df['High'],
	                                   low=df['Low'],
	                                   close=df['Close'],
	                                   length=supertrend_high_length,
	                                   multiplier=supertrend_high_multiplier
	                                   )
	rsi_df = ta.rsi(close=df['Close'],
	                length=rsi_length)
	ema = ta.ema(close=df['Close'],
	             length=ema_length)

	df = df.join(supertrend_df)
	df = df.join(high_supertrend_df)
	df = df.join(rsi_df)
	df = df.join(ema)

	signals_df = pd.DataFrame(columns=['Position', 'Time', 'Entry', 'Stoploss', 'Takeprofit'])

	for index in range(len(df) - 1):
		if df[f'SUPERTd_{supertrend_low_length}_{supertrend_low_multiplier}'][index] != \
				df[f'SUPERTd_{supertrend_low_length}_{supertrend_low_multiplier}'][index + 1]:

			trade_position = None

			entry_price = df['Close'][index + 1]

			if df[f'SUPERTd_{supertrend_low_length}_{supertrend_low_multiplier}'][index] == -1 \
					and df[f'SUPERTd_{supertrend_high_length}_{supertrend_high_multiplier}'][index + 1] == 1 \
					and df[f'RSI_{rsi_length}'][index + 1] > rsi_value_buy \
					and df[f'EMA_{ema_length}'][index + 1] < entry_price:
				stop_loss_price = df[f'Low'][index]
				trade_position = 'buy'
				take_profit_price = entry_price + (entry_price - stop_loss_price) * sl_tp_ratio
				tp_sl_rate = abs(1 - stop_loss_price / take_profit_price)

			if df[f'SUPERTd_{supertrend_low_length}_{supertrend_low_multiplier}'][index] == 1 \
					and df[f'SUPERTd_{supertrend_high_length}_{supertrend_high_multiplier}'][index + 1] == -1 \
					and df[f'RSI_{rsi_length}'][index + 1] < rsi_value_sell \
					and df[f'EMA_{ema_length}'][index + 1] > entry_price:
				stop_loss_price = df['High'][index]
				trade_position = 'sell'
				take_profit_price = entry_price - (stop_loss_price - entry_price) * sl_tp_ratio
				tp_sl_rate = abs(1 - stop_loss_price / take_profit_price)

			if trade_position and tp_sl_rate > 0.006:
				signals_df = signals_df._append({'Position': trade_position,
				                                 'Time': df['Time'][index + 1],
				                                 'Entry': entry_price,
				                                 'Stoploss': round(stop_loss_price, 1),
				                                 'Takeprofit': round(take_profit_price, 1),
				                                 'RSI': df[f'RSI_{rsi_length}'][index + 1],
				                                 'TP_SL_rate': tp_sl_rate},
				                                ignore_index=True
				                                )

	return signals_df


def calculate_statistic(df, signals_df):
	trades_df = pd.DataFrame(columns=['Position', 'Time', 'Entry', 'Stoploss', 'Takeprofit', 'Exit', 'Success', 'RSI'])
	singlal_df_len = len(signals_df)
	count_tp_deals = 0
	for index in range(singlal_df_len - 1):
		success = None
		trade_position = signals_df['Position'][index]
		entry_price = signals_df['Entry'][index]
		entry_time = signals_df['Time'][index]
		take_profit_price = signals_df['Takeprofit'][index]
		stop_loss_price = signals_df['Stoploss'][index]
		rsi = signals_df['RSI'][index]
		tp_sl_rate = signals_df['TP_SL_rate'][index]
		filtered_df = df[df['Time'] > entry_time].reset_index(drop=True)

		deals_amount = len(filtered_df)

		for row in range(deals_amount - 1):
			if trade_position == 'buy' and filtered_df['Low'][row] <= stop_loss_price:
				success = 'SL'

			if trade_position == 'buy' and filtered_df['High'][row] >= take_profit_price:
				success = 'TP'
				count_tp_deals += 1

			if trade_position == 'sell' and filtered_df['High'][row] >= stop_loss_price:
				success = 'SL'

			if trade_position == 'sell' and filtered_df['Low'][row] <= take_profit_price:
				success = 'TP'
				count_tp_deals += 1

			if success:
				trades_df = trades_df._append({'Position': trade_position,
				                               'Time': datetime.utcfromtimestamp(entry_time / 1000),
				                               'Entry': entry_price,
				                               'Stoploss': stop_loss_price,
				                               'Takeprofit': take_profit_price,
				                               'Success': success,
				                               'Exit': datetime.utcfromtimestamp(filtered_df['Time'][row] / 1000),
				                               'RSI': rsi,
				                               'TP_SL_RATE': tp_sl_rate},
				                              ignore_index=True)
				break
	print(round(count_tp_deals / singlal_df_len, 3), timeframe, sl_tp_ratio, ema_length, rsi_length, rsi_value_sell, rsi_value_buy,
	      supertrend_low_length, supertrend_low_multiplier, supertrend_high_length, supertrend_high_multiplier)
	return trades_df


def main():
	df = pd.read_csv(f'{pair}_{timeframe}.csv').iloc[::-1].reset_index(drop=True)

	signals_df = create_signal_df(df)
	trades_df = calculate_statistic(df, signals_df)
	trades_df.to_csv(f'{pair}_trades_{timeframe}.csv')


if __name__ == '__main__':
	timeframe = 240
	sl_tp_ratio = 3

	ema_length = 200

	rsi_length = 14
	rsi_value_sell = 40
	rsi_value_buy = 60

	supertrend_low_length = 2
	supertrend_low_multiplier = 0.5

	supertrend_high_length = 7
	supertrend_high_multiplier = 7.0

	pair = 'BTCUSDT'

	main()
