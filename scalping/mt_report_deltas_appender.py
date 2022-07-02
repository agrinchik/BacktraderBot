import pandas as pd
import os
import glob
from datetime import timedelta
from scalping.binance_trade_data import BinanceTradeDataDownloader

SERVER_TO_LOCAL_TZ_ADJUST_TIMEDELTA = timedelta(hours=3)

TRADES_DOWNLOAD_START_DELTA = timedelta(minutes=90)
TRADES_DOWNLOAD_END_DELTA = timedelta(minutes=5)

DEFAULT_WORKING_PATH = "/Users/alex/Downloads"
ALL_TRADES_FILENAME = "all-trades.xlsx"
ALL_TRADES_DELTAS_FILENAME = "all-trades-deltas.xlsx"

TRADE_LOOKUP_WINDOW_SEC = 5

FIELD_COIN_DELTA_1 = 'd5m'
FIELD_COIN_DELTA_2 = 'd15m'
FIELD_COIN_DELTA_3 = 'd60m'
FIELD_BTC_DELTA_1  = 'dBTC5m'
FIELD_BTC_DELTA_2  = 'dBTC15m'
FIELD_BTC_DELTA_3  = 'dBTC60m'

HEADER_OPEN_COIN_DELTA_1  = "Open:{}".format(FIELD_COIN_DELTA_1)
HEADER_OPEN_COIN_DELTA_2  = "Open:{}".format(FIELD_COIN_DELTA_2)
HEADER_OPEN_COIN_DELTA_3  = "Open:{}".format(FIELD_COIN_DELTA_3)
HEADER_OPEN_BTC_DELTA_1   = "Open:{}".format(FIELD_BTC_DELTA_1)
HEADER_OPEN_BTC_DELTA_2   = "Open:{}".format(FIELD_BTC_DELTA_2)
HEADER_OPEN_BTC_DELTA_3   = "Open:{}".format(FIELD_BTC_DELTA_3)
HEADER_CLOSE_COIN_DELTA_1 = "Close:{}".format(FIELD_COIN_DELTA_1)
HEADER_CLOSE_COIN_DELTA_2 = "Close:{}".format(FIELD_COIN_DELTA_2)
HEADER_CLOSE_COIN_DELTA_3 = "Close:{}".format(FIELD_COIN_DELTA_3)
HEADER_CLOSE_BTC_DELTA_1  = "Close:{}".format(FIELD_BTC_DELTA_1)
HEADER_CLOSE_BTC_DELTA_2  = "Close:{}".format(FIELD_BTC_DELTA_2)
HEADER_CLOSE_BTC_DELTA_3  = "Close:{}".format(FIELD_BTC_DELTA_3)


class MTReportDeltaAppender(object):
    def __init__(self):
        self.trade_downloader = None
        self._model_dict = {}
        self._total_stats_dict = {}

    def whereAmI(self):
        return os.path.dirname(os.path.realpath(__import__("__main__").__file__))

    def get_trades_filename(self):
        file_search_wildcard = "{}/*{}".format(DEFAULT_WORKING_PATH, ALL_TRADES_FILENAME)
        paths = glob.glob(file_search_wildcard)
        if len(paths) == 0:
            raise Exception("No {} files found".format(file_search_wildcard))
        if len(paths) > 1:
            raise Exception("Multiple {} files found. Need to have only 1 file to process.".format(file_search_wildcard))
        return paths[0]

    def get_output_analysis_filename(self, trades_filename):
        fn = trades_filename.split("/")[-1]
        prefix = fn.split(ALL_TRADES_FILENAME)[0]
        return '{}/{}{}'.format(DEFAULT_WORKING_PATH, prefix, ALL_TRADES_DELTAS_FILENAME)

    def read_trades_data(self, trades_filename):
        try:
            df = pd.read_excel(trades_filename)
            return df
        except:
            print("!!! Error during opening {} file.".format(trades_filename))

    def get_datetime_local_tz(self, row, column_name):
        if isinstance(row, pd.DataFrame):
            return pd.to_datetime(row[column_name].values[0]) + SERVER_TO_LOCAL_TZ_ADJUST_TIMEDELTA
        elif isinstance(row, pd.Series):
            return pd.to_datetime(row[column_name]) + SERVER_TO_LOCAL_TZ_ADJUST_TIMEDELTA

    def get_download_daterange(self, trades_data_df, symbol):
        symbol_df = trades_data_df[trades_data_df['symbol'] == symbol]
        start_datetime = self.get_datetime_local_tz(symbol_df.head(1), "entry_timestamp") - TRADES_DOWNLOAD_START_DELTA
        end_datetime = self.get_datetime_local_tz(symbol_df.tail(1), "close_timestamp") + TRADES_DOWNLOAD_END_DELTA
        start_datetime_str = start_datetime.strftime("%Y-%m-%dT%H:%M:%S")
        end_datetime_str = end_datetime.strftime("%Y-%m-%dT%H:%M:%S")
        return {"start": start_datetime_str, "end": end_datetime_str}

    def append_deltas(self, trades_data_df, symbol, is_future):
        trades_symbol_df = trades_data_df[trades_data_df['symbol'] == symbol]
        dirname = self.whereAmI()
        tick_data_filepath = self.trade_downloader.get_tick_data_filepath(dirname, symbol, is_future)
        tick_data_filename = self.trade_downloader.get_tick_data_filename(tick_data_filepath, symbol)
        tick_data_df = pd.read_csv(tick_data_filename).set_index('Timestamp')

        for index, row in trades_symbol_df.iterrows():
            open_trade_datetime = pd.to_datetime(row["entry_timestamp"])
            open_trade_timestamp_start_msec = int(open_trade_datetime.timestamp() * 1000)
            open_trade_timestamp_end_msec = int(open_trade_timestamp_start_msec + TRADE_LOOKUP_WINDOW_SEC * 1000)
            open_ticks_df = tick_data_df.loc[(tick_data_df.index >= open_trade_timestamp_start_msec) & (tick_data_df.index <= open_trade_timestamp_end_msec)]
            if len(open_ticks_df) > 0:
                tick_row = open_ticks_df.head(1).iloc[0]
                open_cond = (trades_data_df["entry_timestamp"] == row["entry_timestamp"]) & (trades_data_df["symbol"] == symbol)
                trades_data_df.loc[open_cond, HEADER_OPEN_COIN_DELTA_1] = tick_row[FIELD_COIN_DELTA_1]
                trades_data_df.loc[open_cond, HEADER_OPEN_COIN_DELTA_2] = tick_row[FIELD_COIN_DELTA_2]
                trades_data_df.loc[open_cond, HEADER_OPEN_COIN_DELTA_3] = tick_row[FIELD_COIN_DELTA_3]
                trades_data_df.loc[open_cond, HEADER_OPEN_BTC_DELTA_1]  = tick_row[FIELD_BTC_DELTA_1]
                trades_data_df.loc[open_cond, HEADER_OPEN_BTC_DELTA_2]  = tick_row[FIELD_BTC_DELTA_2]
                trades_data_df.loc[open_cond, HEADER_OPEN_BTC_DELTA_3]  = tick_row[FIELD_BTC_DELTA_3]

            close_trade_datetime = pd.to_datetime(row["close_timestamp"])
            close_trade_timestamp_start_msec = int(close_trade_datetime.timestamp() * 1000)
            close_trade_timestamp_end_msec = int(close_trade_timestamp_start_msec + TRADE_LOOKUP_WINDOW_SEC * 1000)
            close_ticks_df = tick_data_df.loc[(tick_data_df.index >= close_trade_timestamp_start_msec) & (tick_data_df.index <= close_trade_timestamp_end_msec)]
            if len(close_ticks_df) > 0:
                tick_row = close_ticks_df.head(1).iloc[0]
                close_cond = (trades_data_df["close_timestamp"] == row["close_timestamp"]) & (trades_data_df["symbol"] == symbol)
                trades_data_df.loc[close_cond, HEADER_CLOSE_COIN_DELTA_1] = tick_row[FIELD_COIN_DELTA_1]
                trades_data_df.loc[close_cond, HEADER_CLOSE_COIN_DELTA_2] = tick_row[FIELD_COIN_DELTA_2]
                trades_data_df.loc[close_cond, HEADER_CLOSE_COIN_DELTA_3] = tick_row[FIELD_COIN_DELTA_3]
                trades_data_df.loc[close_cond, HEADER_CLOSE_BTC_DELTA_1]  = tick_row[FIELD_BTC_DELTA_1]
                trades_data_df.loc[close_cond, HEADER_CLOSE_BTC_DELTA_2]  = tick_row[FIELD_BTC_DELTA_2]
                trades_data_df.loc[close_cond, HEADER_CLOSE_BTC_DELTA_3]  = tick_row[FIELD_BTC_DELTA_3]

        return trades_data_df

    def add_deltas_columns(self, df):
        df_new = df.assign(oc1=0, oc2=0, oc3=0, ob1=0, ob2=0, ob3=0, cc1=0, cc2=0, cc3=0, cb1=0, cb2=0, cb3=0)
        df_new.rename(columns={"oc1": HEADER_OPEN_COIN_DELTA_1, "oc2": HEADER_OPEN_COIN_DELTA_2, "oc3": HEADER_OPEN_COIN_DELTA_3, "ob1": HEADER_OPEN_BTC_DELTA_1, "ob2": HEADER_OPEN_BTC_DELTA_2, "ob3": HEADER_OPEN_BTC_DELTA_3,
                               "cc1": HEADER_CLOSE_COIN_DELTA_1, "cc2": HEADER_CLOSE_COIN_DELTA_2, "cc3": HEADER_CLOSE_COIN_DELTA_3, "cb1": HEADER_CLOSE_BTC_DELTA_1, "cb2": HEADER_CLOSE_BTC_DELTA_2, "cb3": HEADER_CLOSE_BTC_DELTA_3
                               }, inplace=True)
        return df_new

    def run(self, is_future):
        trades_filename = self.get_trades_filename()
        trades_data_df = self.read_trades_data(trades_filename)
        trades_data_df = self.add_deltas_columns(trades_data_df)
        symbols = list(trades_data_df['symbol'].unique())
        symbols.sort()

        for symbol in symbols:
            symbol_daterange = self.get_download_daterange(trades_data_df, symbol)

            self.trade_downloader = BinanceTradeDataDownloader()
            print("Start downloading tick data for {}...".format(symbol))
            self.trade_downloader.process(symbol, symbol_daterange["start"], symbol_daterange["end"], is_future)
            print("Finished downloading tick data for {}!".format(symbol))

            trades_data_df = self.append_deltas(trades_data_df, symbol, is_future)

        output_filename = self.get_output_analysis_filename(trades_filename)
        trades_data_df.to_excel(output_filename, engine='xlsxwriter')
        print("Written modified file: {}".format(output_filename))


def main():
    d = MTReportDeltaAppender()
    d.run(True)


if __name__ == '__main__':
    main()
