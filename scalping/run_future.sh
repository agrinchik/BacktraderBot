#! /bin/bash

if [ -d "/Users/alex/opt/anaconda3" ]; then
    source /Users/alex/opt/anaconda3/etc/profile.d/conda.sh
elif [ -d "/home/alex/anaconda3" ]; then
    source /home/alex/anaconda3/etc/profile.d/conda.sh
elif [ -d "/Users/alex/anaconda3" ]; then
    source /Users/alex/anaconda3/etc/profile.d/conda.sh
elif [ -d "/c/Anaconda3" ]; then
    source /c/Anaconda3/etc/profile.d/conda.sh
fi
conda activate Backtrader

declare -a excluded_symbols_regex="BTCUSDT BTCSTUSDT TUSDT"

python get_symbols.py -q ${2} -f

declare -a symbol_list
while read line; do
    symbol_list+=($line)
done < symbols_future_${2}.txt

declare -a start_minutes_ago=$(($1*60))

declare -a ultrashortmode=${3}

declare -a future_flag="-f"

declare -a moonbot_flag="-b"

now_timestamp="$(date +'%s')"
now_timestamp=$((now_timestamp - now_timestamp % 60))

start_timestamp=$((now_timestamp - 60 * start_minutes_ago))
start_date="$(date -j -f "%s" "${start_timestamp}" "+%Y-%m-%dT%H:%M:%S")"
end_timestamp=$((start_timestamp + 60 * start_minutes_ago))
end_date="$(date -j -f "%s" "${end_timestamp}" "+%Y-%m-%dT%H:%M:%S")"

output_folder_prefix="$(date -j -f "%s" "${end_timestamp}" "+%Y%m%d_%H%M")"
BASE_OUT_FOLDER=../../../../../../../_TEMP/scalping/out/strategies
output_folder=${BASE_OUT_FOLDER}/${output_folder_prefix}_Future_${start_minutes_ago}m/

echo Deleting old data files...
rm -rf ./../marketdata/shots/binance/future/*
rm -rf ./../marketdata/tradedata/binance/future/*
echo Done!

echo Detecting shots for the last $start_minutes_ago minutes:
echo $start_date
echo $end_date

for symbol in "${symbol_list[@]}"
do
    if printf "${excluded_symbols_regex}" | grep -q ${symbol}; then
        echo Skipping excluded symbol: ${symbol} ...
            continue
    fi
    # Download tick trade data for all symbols
    python binance_trade_data.py -s $symbol -t $start_date -e $end_date $future_flag

    # Detect shots information for all symbols
    python shots_detector.py ${ultrashortmode} -e binance -s $symbol $future_flag $moonbot_flag

    # Calculate best PnL for all the shots
    python calc_shots_pnl.py ${ultrashortmode} -e binance -s $symbol $future_flag $moonbot_flag
done


# Generate strategy files for MB/MT
cd ..
python -m scalping.strategy_generator -e binance $future_flag $moonbot_flag
cd scalping

mkdir $output_folder
cp ./../marketdata/shots/binance/future/* $output_folder
cp ./../marketdata/shots/binance/future/algorithms.config_future $output_folder/../