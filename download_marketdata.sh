#! /bin/bash

if [ -d "/Users/alex/opt/anaconda3" ]; then
    source /Users/alex/opt/anaconda3/etc/profile.d/conda.sh
elif [ -d "/home/alex/anaconda3" ]; then
    source /home/alex/anaconda3/etc/profile.d/conda.sh
elif [ -d "/Users/alex/anaconda3" ]; then
    source /Users/alex/anaconda3/etc/profile.d/conda.sh
fi
conda activate Backtrader

declare exchange="binance"

#declare -a arr_symbols=( "BTC/USD" "ETH/USD" "LTC/USD" "XRP/USD" "ETC/USD" "IOTA/USD" "EOS/USD" "NEO/USD" "ZEC/USD" "ETP/USD" "XMR/USD"  "DASH/USD")
#declare -a arr_symbols=("OP/USDT" "LPT/USDT" "TRB/USDT" "RLC/USDT" "PEOPLE/USDT" "ROSE/USDT" "FLOW/USDT" "UNFI/USDT" "GAL/USDT" "BLZ/USDT")
#declare -a arr_symbols=("XRP/BUSD")
declare -a arr_symbols=("DOGE/BUSD")

#declare -a arr_timeframes=("1m" "5m" "15m" "30m" "1h" "3h" "6h" "12h" "1d")
declare -a arr_timeframes=("1m")

declare -a start_days_ago=$(($1))
now_timestamp="$(date +'%s')"
start_timestamp=$((now_timestamp - (now_timestamp % (3600 * 24)) - 3600 * 24 * start_days_ago))
start_date="$(date -j -f "%s" "${start_timestamp}" "+%Y-%m-%dT%H:%M:%S")"
end_date="$(date -j -f "%s" "${now_timestamp}" "+%Y-%m-%dT%H:%M:%S")"

echo start_date=$start_date
echo end_date=$end_date

for symbol in "${arr_symbols[@]}"
do

    for timeframe in "${arr_timeframes[@]}"
    do
        echo "Downloading market data for $exchange/$symbol/$timeframe..."
        current_date_time="`date '+%Y-%m-%d - %H:%M:%S'`"
        echo "Started: $current_date_time... "
        python ccxt_market_data.py -s $symbol -e $exchange -t $timeframe -r $start_date -n $end_date
        current_date_time="`date '+%Y-%m-%d - %H:%M:%S'`"
        echo "Finished: $current_date_time."
    done
done
