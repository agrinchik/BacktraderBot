from datetime import datetime
import argparse
import pandas as pd
import os

EXCHANGE_STR = "binance"

ENABLE_VERIFY = False


def parse_args():
    parser = argparse.ArgumentParser(description='Binance ticks to OHLC candles converter')

    parser.add_argument('-s', '--symbol',
                        type=str,
                        required=True,
                        help='The Symbol of the Instrument/Currency Pair')

    parser.add_argument('-f', '--future',
                        action='store_true',
                        help=('Is instrument of future type?'))

    parser.add_argument('-g', '--granularity',
                        type=int,
                        required=True,
                        help='OHLC timeframe granularity in milliseconds (e.g. 100)')

    parser.add_argument('--debug',
                        action='store_true',
                        help=('Print Sizer Debugs'))

    return parser.parse_args()


class OhlcCandle(object):
    def __init__(self, timestamp_v, open_v, high_v, low_v, close_v):
        self.timestamp = timestamp_v
        self.open = open_v
        self.high = high_v
        self.low = low_v
        self.close = close_v
        self.volume = 0

    def get_table_data(self):
        return [
            self.to_datetime(self.timestamp), self.open, self.high, self.low, self.close, self.volume
        ]

    @staticmethod
    def create_new(timestamp_v, tick_price):
        return OhlcCandle(timestamp_v, tick_price, tick_price, tick_price, tick_price)

    @staticmethod
    def create_repeat(timestamp_v, ohlc_candle):
        return OhlcCandle(timestamp_v, ohlc_candle.close, ohlc_candle.close, ohlc_candle.close, ohlc_candle.close)

    def to_datetime(self, timestamp):
        return "{}.{:03d}".format(datetime.fromtimestamp(int(timestamp / 1000)).strftime("%Y-%m-%dT%H:%M:%S"), timestamp % 1000)


class Ticks2OhlcCandlesConverter(object):
    def __init__(self):
        pass

    def get_symbol_type_str(self, is_future):
        if is_future:
            return "future"
        else:
            return "spot"

    def whereAmI(self):
        return os.path.dirname(os.path.realpath(__import__("__main__").__file__))

    def get_tick_data_filepath(self, basedir, symbol, is_future):
        symbol_type_str = self.get_symbol_type_str(is_future)
        return '{}/../marketdata/tradedata/{}/{}/{}'.format(basedir, EXCHANGE_STR, symbol_type_str, symbol)

    def get_tick_data_filename(self, output_path, symbol):
        return '{}/{}-{}.csv'.format(output_path, EXCHANGE_STR, symbol)

    def get_output_data_filepath(self, basedir, symbol, granularity):
        return '{}/../marketdata/{}/{}/{}ms'.format(basedir, EXCHANGE_STR, symbol, granularity)

    def get_output_filename(self, output_path, symbol, granularity):
        return '{}/{}-{}-{}ms.csv'.format(output_path, EXCHANGE_STR, symbol, granularity)

    def read_csv_data(self, filepath):
        try:
            df = pd.read_csv(filepath)
            if len(df) == 0:
                raise Exception('Input tick data file missing.')
        except Exception as e:
            raise Exception('Input tick data file missing.')
        return df

    def define_base_timestamp(self, timestamp, granularity_md):
        remainder = timestamp % 1000
        return int(timestamp / 1000) * 1000 + granularity_md * int(remainder / granularity_md)

    def update_candle(self, ohlc_candle, tick_price):
        if tick_price > ohlc_candle.high:
            ohlc_candle.high = tick_price

        if tick_price < ohlc_candle.low:
            ohlc_candle.low = tick_price

        ohlc_candle.close = tick_price

    def create_multiple_candles(self, last_candle, new_tick_timestamp, new_tick_price, granularity_ms):
        out_arr = []
        c_timestamp = last_candle.timestamp + granularity_ms
        while c_timestamp <= new_tick_timestamp:
            if new_tick_timestamp - c_timestamp >= granularity_ms:
                ohlc_candle = OhlcCandle.create_repeat(c_timestamp, last_candle)
            else:
                last_close_price = out_arr[-1].close if len(out_arr) > 0 else last_candle.close
                ohlc_candle = OhlcCandle.create_new(c_timestamp, last_close_price)
                self.update_candle(ohlc_candle, new_tick_price)
            out_arr.append(ohlc_candle)
            c_timestamp += granularity_ms

        return out_arr

    def convert(self, tick_df, granularity_ms):
        first_row = tick_df.head(1)
        first_timestamp = first_row["Timestamp"].values[0]
        base_timestamp = self.define_base_timestamp(first_timestamp, granularity_ms)
        ohlc_out_arr = []

        total_num = len(tick_df)
        print("Number of ticks: {}".format(total_num))
        for index, row in tick_df.iterrows():
            if index % 10000 == 0:
                print("Processed {} ticks".format(index))
            tick_timestamp = row["Timestamp"]
            tick_price = row["Price"]
            if len(ohlc_out_arr) == 0:
                last_candle = OhlcCandle.create_new(base_timestamp, tick_price)
                ohlc_out_arr.append(last_candle)
            else:
                last_candle = ohlc_out_arr[-1]

            timestamp_delta = tick_timestamp - last_candle.timestamp
            if timestamp_delta < granularity_ms:
                self.update_candle(last_candle, tick_price)
            else:
                multiple_candles = self.create_multiple_candles(last_candle, tick_timestamp, tick_price, granularity_ms)
                ohlc_out_arr.extend(multiple_candles)

        ohlc_table_arr = []
        for ohlc_candle in ohlc_out_arr:
            ohlc_table_arr.append(ohlc_candle.get_table_data())

        print("Total number of output candles: {}".format(len(ohlc_table_arr)))

        return pd.DataFrame(ohlc_table_arr, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']).set_index('Timestamp')

    def check_equals(self, val1, val2):
        if val1 != val2:
            raise Exception("Values {} and {} are not equal!".format(val1, val2))

    def check_min_max(self, df, timestamp1, timestamp2, open_v, check_high_v, check_low_v):
        ticks_df = df.loc[(df.index >= timestamp1) & (df.index < timestamp2)]
        if len(ticks_df) > 0:
            self.check_equals(max(open_v, ticks_df['Price'].max()), check_high_v)
            self.check_equals(min(open_v, ticks_df['Price'].min()), check_low_v)
        else:
            self.check_equals(open_v, check_high_v)
            self.check_equals(open_v, check_low_v)

    def verify(self, input_df, output_df, granularity_msec):
        if not ENABLE_VERIFY:
            return

        print("\nVerifying output candles...")

        input_df = input_df.set_index('Timestamp')
        input_first_row = input_df.head(1)
        input_first_price = input_first_row["Price"].values[0]
        input_last_row = input_df.tail(1)
        input_last_price = input_last_row["Price"].values[0]
        output_first_row = output_df.head(1)
        output_first_open = output_first_row["Open"].values[0]
        output_first_high = output_first_row["High"].values[0]
        output_first_low = output_first_row["Low"].values[0]
        output_first_close = output_first_row["Close"].values[0]
        output_first_timestamp = int(pd.to_datetime(output_first_row.index.values[0]).to_pydatetime().timestamp() * 1000)
        output_last_row = output_df.tail(1)
        output_last_close = output_last_row["Close"].values[0]

        self.check_equals(input_first_price, output_first_open)
        self.check_equals(input_last_price, output_last_close)

        prev_timestamp = output_first_timestamp
        prev_open = output_first_open
        prev_high = output_first_high
        prev_low = output_first_low
        prev_close = output_first_close
        c = 0
        for index, row in output_df.iloc[1:].iterrows():
            if c % 10000 == 0:
                print("Verified {} output candles".format(c))
            # print("index={}".format(index))
            curr_timestamp = int(pd.to_datetime(index).to_pydatetime().timestamp() * 1000)
            curr_open = row["Open"]
            curr_high = row["High"]
            curr_low = row["Low"]
            curr_close = row["Close"]
            self.check_equals(prev_timestamp + granularity_msec, curr_timestamp)
            self.check_equals(prev_close, curr_open)
            self.check_min_max(input_df, prev_timestamp, curr_timestamp, prev_open, prev_high, prev_low)
            prev_timestamp = curr_timestamp
            prev_open = curr_open
            prev_high = curr_high
            prev_low = curr_low
            prev_close = curr_close
            c += 1

    def process(self, symbol, is_future, granularity_msec):
        dirname = self.whereAmI()
        input_path = self.get_tick_data_filepath(dirname, symbol, is_future)
        input_filename = self.get_tick_data_filename(input_path, symbol)

        print("Processing tick trade data file:\n {}\n....".format(input_filename))

        tick_df = self.read_csv_data(input_filename)

        output_df = self.convert(tick_df, granularity_msec)

        self.verify(tick_df, output_df, granularity_msec)

        output_path = self.get_output_data_filepath(dirname, symbol, granularity_msec)
        os.makedirs(output_path, exist_ok=True)
        output_filename = self.get_output_filename(output_path, symbol, granularity_msec)
        output_df.to_csv(output_filename)

        print("Converted tick trade data to {}ms OHLC candles:\n {}".format(granularity_msec, output_filename))


def main():
    # Get arguments
    args = parse_args()

    d = Ticks2OhlcCandlesConverter()
    d.process(args.symbol, args.future, args.granularity)


if __name__ == '__main__':
    main()
