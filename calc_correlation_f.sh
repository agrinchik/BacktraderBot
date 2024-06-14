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
declare timeframe="1m"
declare quote_currency="USDT"
declare symbol_file=symbols_future_${quote_currency}.txt

declare -a start_days_ago=$(($1))
now_timestamp="$(date +'%s')"
start_timestamp=$((now_timestamp - (now_timestamp % (3600 * 24)) - 3600 * 24 * start_days_ago))
end_timestamp=$((now_timestamp - (now_timestamp % (3600 * 24))))
start_date="$(date -j -f "%s" "${start_timestamp}" "+%Y-%m-%dT%H:%M:%S")"
end_date="$(date -j -f "%s" "${end_timestamp}" "+%Y-%m-%dT%H:%M:%S")"

echo start_date=$start_date
echo end_date=$end_date

declare -a arr_symbols

cd ./scalping
python get_symbols.py -q ${quote_currency} -f

while read line; do
    symb=$line
    len1=${#symb}
    len2=${#quote_currency}
    new_symbol="${symb::len1-len2}/${quote_currency}"
    arr_symbols+=(${new_symbol})
done < ${symbol_file}
cd ..

echo "Deleting old market data in ./marketdata/$exchange"
rm -rf ./marketdata/$exchange

echo "Deleting previous result file ./marketdata/correlation/correlation_f.csv"
rm -rf ./marketdata/correlation/correlation_f.csv

for symbol in "${arr_symbols[@]}"
do
    mod_symbol=${symbol//[\/]/}
    echo "Downloading market data for $exchange/$mod_symbol/$timeframe..."
    current_date_time="`date '+%Y-%m-%d - %H:%M:%S'`"
    echo "Started: $current_date_time... "
    python ccxt_market_data_f.py -s $symbol -e $exchange -t $timeframe -r $start_date -n $end_date
    current_date_time="`date '+%Y-%m-%d - %H:%M:%S'`"
    echo "Finished: $current_date_time."
done

echo Running Correlation Calculation...
python -m correlation.correlation -e $exchange -t $timeframe -f $symbol_file
