'''
Correlation Calculator
'''

import argparse
import os
import pandas as pd


class CorrelationCalculator(object):

    def __init__(self):
        self._output_file_full_name = None

    def parse_args(self):
        parser = argparse.ArgumentParser(description='Correlation Calculator')

        parser.add_argument('-e', '--exchange',
                            type=str,
                            required=True,
                            help='The exchange name')

        parser.add_argument('-t', '--timeframe',
                            type=str,
                            required=True,
                            help='The timeframe')

        parser.add_argument('-f', '--symbolfile',
                            type=str,
                            default="",
                            required=True,
                            help='File containing list of symbols to process')

        parser.add_argument('--debug',
                            action='store_true',
                            help=('Print Debugs'))

        return parser.parse_args()

    def get_input_filename(self, args):
        return './scalping/{}'.format(args.symbolfile)

    def get_output_path(self, base_dir):
        return './marketdata/correlation'

    def get_output_filename(self, base_path):
        return '{}/correlation_f.csv'.format(base_path)

    def whereAmI(self):
        return os.path.dirname(os.path.realpath(__import__("__main__").__file__))

    def get_marketdata_filename(self, exchange, symbol, timeframe):
        return './marketdata/{}/{}/{}/{}-{}-{}.csv'.format(exchange, symbol, timeframe, exchange, symbol, timeframe)

    def init_output_files(self):
        base_dir = self.whereAmI()
        output_path = self.get_output_path(base_dir)
        os.makedirs(output_path, exist_ok=True)

        self._output_file_full_name = self.get_output_filename(output_path)

    def get_symbol_list(self, input_filename):
        with open(input_filename) as file:
            symbols = [line.rstrip() for line in file]
        return symbols

    def round_value(self, x):
        return round(x, 2)

    def run(self):
        args = self.parse_args()

        input_filename = self.get_input_filename(args)

        symbols = self.get_symbol_list(input_filename)

        all_symbols = []
        for symbol in symbols:
            marketdata_filename = self.get_marketdata_filename(args.exchange, symbol, args.timeframe)

            try:
                df = pd.read_csv(marketdata_filename, usecols=['Timestamp', 'Close'])
                df['Symbol'] = symbol
                all_symbols.append(df)
            except Exception as e:
                print("Unable to open file: {}".format(marketdata_filename))

        # concatenate into df
        df = pd.concat(all_symbols)
        df = df.reset_index()
        df = df[['Timestamp', 'Close', 'Symbol']]
        df.head()

        # make pivot table
        df_pivot = df.pivot('Timestamp', 'Symbol', 'Close').reset_index()
        df_pivot.head()

        # calculate correlation
        corr_df = df_pivot.corr(method='pearson')

        corr_df = corr_df.applymap(self.round_value)

        self.init_output_files()

        # Save results
        print("*** Writing correlation results into file: {}".format(self._output_file_full_name))
        corr_df.to_csv(self._output_file_full_name)


def main():
    calculator = CorrelationCalculator()
    calculator.run()


if __name__ == '__main__':
    main()
