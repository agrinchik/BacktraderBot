import ccxt
from datetime import datetime
import argparse
import pandas as pd
import time
import pytz
import os

EXCHANGE_STR = "binance"


def parse_args():
    parser = argparse.ArgumentParser(description='CCXT FUTURE Market Data Downloader')

    parser.add_argument('-s', '--symbol',
                        type=str,
                        required=True,
                        help='The Symbol of the Instrument/Currency Pair To Download')

    parser.add_argument('-e', '--exchange',
                        type=str,
                        required=True,
                        help='The exchange to download from')

    parser.add_argument('-t', '--timeframe',
                        type=str,
                        default='1d',
                        choices=['1m', '5m', '15m', '30m', '1h', '2h', '3h', '4h', '6h', '12h', '1d', '1M', '1y'],
                        help='The timeframe to download')

    parser.add_argument('-r', '--start',
                        type=str,
                        required=True,
                        help='Start datetime')

    parser.add_argument('-n', '--end',
                        type=str,
                        required=True,
                        help='End datetime')

    parser.add_argument('--debug', action='store_true', help=('Print Debugs'))

    return parser.parse_args()


def resolve_instrument(symbol):
    quote_curr = None
    if symbol.endswith("USDT"):
        quote_curr = "USDT"
    elif symbol.endswith("BTC"):
        quote_curr = "BTC"
    elif symbol.endswith("BNB"):
        quote_curr = "BNB"
    elif symbol.endswith("BUSD"):
        quote_curr = "BUSD"
    elif symbol.endswith("TUSD"):
        quote_curr = "TUSD"

    if quote_curr:
        l = len(quote_curr)
        return '{0}/{1}'.format(symbol[:-l], symbol[-l:])
    else:
        return symbol

# Get our arguments
args = parse_args()

# Get our Exchange
try:
    w_exchange = ccxt.binance({
        'enableRateLimit': True,
        'rateLimit': 5,
        'options': {
            'defaultType': 'future',
        }
    })
except AttributeError:
    print('-' * 36, ' ERROR ', '-' * 35)
    print('Exchange "{}" not found. Please check the exchange is supported.'.format(args.exchange))
    print('-' * 80)
    quit()

# Check if the symbol is available on the Exchange
w_exchange.load_markets()

if args.symbol not in w_exchange.symbols:
    print('-' * 36, ' ERROR ', '-' * 35)
    print('The requested symbol ({}) is not available from {}\n'.format(args.symbol, EXCHANGE_STR))
    print('Available symbols are:')
    for key in w_exchange.symbols:
        print('  - ' + key)
    print('-' * 80)
    quit()

symbol_out = args.symbol.replace("/", "")

# Check if fetching of OHLC Data is supported
if w_exchange.has["fetchOHLCV"] == False:
    print('-'*36,' ERROR ','-'*35)
    print('{} does not support fetching OHLC data. Please use another exchange'.format(args.exchange))
    print('-'*80)
    quit()

# Check requested timeframe is available. If not return a helpful error.
if args.timeframe not in w_exchange.timeframes:
    print('-'*36,' ERROR ','-'*35)
    print('The requested timeframe ({}) is not available from {}\n'.format(args.timeframe,args.exchange))
    print('Available timeframes are:')
    for key in w_exchange.timeframes.keys():
        print('  - ' + key)
    print('-'*80)
    quit()


def whereAmI():
    return os.path.dirname(os.path.realpath(__import__("__main__").__file__))

sym = args.symbol
symbol_out = args.symbol.replace("/", "")
dirname = whereAmI()
output_path = '{}/marketdata/{}/{}/{}'.format(dirname, args.exchange, symbol_out, args.timeframe)
os.makedirs(output_path, exist_ok=True)

# Get data
#    def fetch_ohlcv(self, symbol, timeframe='1m', since=None, limit=None, params={}):

gmt3_tz = pytz.timezone('Etc/GMT-2')
start_utc_date = datetime.strptime(args.start, '%Y-%m-%dT%H:%M:%S')
start_utc_date = gmt3_tz.localize(start_utc_date, is_dst=True)
start = int(start_utc_date.timestamp()) * 1000

end_utc_date = datetime.strptime(args.end, '%Y-%m-%dT%H:%M:%S')
end_utc_date = gmt3_tz.localize(end_utc_date, is_dst=True)
end = int(end_utc_date.timestamp()) * 1000

limit = 1000
timestamp = start
last_timestamp = None
data = []

while timestamp <= end and timestamp != last_timestamp:
    print("Requesting " + datetime.fromtimestamp(int(timestamp/1000)).strftime("%Y-%m-%dT%H:%M:%S"))
    trades = w_exchange.fetch_ohlcv(args.symbol, args.timeframe, timestamp, limit)
    last_timestamp = timestamp
    timestamp = trades[-1][0]
    data.extend(trades)
    time.sleep(0.1)
    if len(data) > 0 and len(trades) > 1:
        del data[-1]

tick_size = w_exchange.markets[args.symbol]['info']['filters'][0]['tickSize']

for t in data:
    t[0] = datetime.fromtimestamp(int(t[0] / 1000)).strftime("%Y-%m-%dT%H:%M:%S")
    t.append(tick_size)

header = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume', 'Tick Size']
df = pd.DataFrame(data, columns=header).set_index('Timestamp')
# Save it
filename = '{}/{}-{}-{}.csv'.format(output_path, args.exchange, symbol_out, args.timeframe)
df.to_csv(filename)