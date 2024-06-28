import requests
import json
import pymysql
import setupmkt
import tradetools
from datetime import datetime

db = pymysql.connect(host='192.168.50.60', user='root', password=setupmkt.MySQL_Password, database='deribit',
                     port=13006)
table_name = 'option_files'
msg = {
    "method": "public/get_instruments",
    "params": {
        "currency": "BTC",
        "kind": "option",
        "expired": True
    },
    "jsonrpc": "2.0"
}
response = requests.post(setupmkt.history_url, json=msg)
data = response.json()
for j in range(len(data['result'])):
    data_dict = data['result'][j]
    instrument_name_ = data_dict['instrument_name']
    print(instrument_name_)
    strike_ = data_dict['strike']
    print(strike_)
    instrument_id_ = tradetools.zero_None(data_dict, "instrument_id")
    tick_size_ = tradetools.zero_None(data_dict, "tick_size")
    taker_commission_ = tradetools.zero_None(data_dict, "taker_commission")
    settlement_period_ = tradetools.zero_None(data_dict, "settlement_period", 'string')
    settlement_currency_ = tradetools.zero_None(data_dict, "settlement_currency", 'string')
    rfq_ = tradetools.zero_None(data_dict, "rfq", 'bool')
    quote_currency_ = tradetools.zero_None(data_dict, "quote_currency", 'string')
    price_index_ = tradetools.zero_None(data_dict, "price_index", 'string')
    option_type_ = tradetools.zero_None(data_dict, "option_type", 'string')
    min_trade_amount_ = tradetools.zero_None(data_dict, "min_trade_amount")
    maker_commission_ = tradetools.zero_None(data_dict, "maker_commission")
    kind_ = tradetools.zero_None(data_dict, "kind", 'string')
    is_active_ = tradetools.zero_None(data_dict, "is_active", 'bool')
    expiration_timestamp_ = tradetools.zero_None(data_dict, "expiration_timestamp")
    creation_timestamp_ = tradetools.zero_None(data_dict, "creation_timestamp")
    counter_currency_ = tradetools.zero_None(data_dict, "counter_currency", 'string')
    contract_size_ = tradetools.zero_None(data_dict, "contract_size")
    block_trade_tick_size_ = tradetools.zero_None(data_dict, "block_trade_tick_size")
    block_trade_min_trade_amount_ = tradetools.zero_None(data_dict, "block_trade_min_trade_amount")
    block_trade_commission_ = tradetools.zero_None(data_dict, "block_trade_commission")
    base_currency_ = tradetools.zero_None(data_dict, "base_currency", 'string')
    cursor0 = db.cursor()
    sql_sentence0 = "INSERT INTO " + table_name + ("(instrument_name, instrument_id, tick_size, taker_commission, "
                                                   "strike, settlement_period, settlement_currency, rfq, "
                                                   "quote_currency, price_index, option_type, min_trade_amount, "
                                                   "maker_commission, kind, is_active, expiration_timestamp, "
                                                   "creation_timestamp, counter_currency, contract_size, "
                                                   "block_trade_tick_size, block_trade_min_trade_amount, "
                                                   "block_trade_commission, base_currency) \
                 VALUES ('%s', %s, %s, %s, %s, '%s', '%s', %s, '%s', '%s', '%s', %s, %s, '%s', %s, %s, %s, '%s', "
                                                   "%s, %s, %s, %s, '%s')") % \
                    (instrument_name_, instrument_id_, tick_size_, taker_commission_, strike_,
                     settlement_period_, settlement_currency_, rfq_, quote_currency_, price_index_, option_type_,
                     min_trade_amount_, maker_commission_, kind_, is_active_, expiration_timestamp_,
                     creation_timestamp_, counter_currency_, contract_size_, block_trade_tick_size_,
                     block_trade_min_trade_amount_, block_trade_commission_, base_currency_)
    try:
        cursor0.execute(sql_sentence0)
        db.commit()
    except Exception as e:
        print("Error: unable to fetch data")
        db.rollback()
    finally:
        cursor0.close()

db.close()
