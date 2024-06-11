import requests
import pandas as pd
import pymysql
from sqlalchemy import create_engine
import setupmkt
import tradetools


def trades_sql(db_, table_name_, msg_):
    has_more_ = False
    end_seq_ = 0
    try:
        response = requests.post(setupmkt.history_url, json=msg_)
        data_ = response.json()
        print(data_)
        data_list = data_['result']['trades']
        data_num = len(data_list)
        print(data_num)
        has_more_ = data_['result']['has_more']
        end_seq_ = data_list[data_num - 1]['trade_seq']
        for j in range(data_num):
            data_dict = data_list[j]
            instrument_name_ = data_dict['instrument_name']
            print(instrument_name_)
            timestamp_ = data_dict['timestamp']
            print(timestamp_)
            trade_id_ = tradetools.zero_None(data_dict, "trade_id", 'string')
            tick_direction_ = tradetools.zero_None(data_dict, "tick_direction")
            trade_seq_ = tradetools.zero_None(data_dict, "trade_seq")
            direction_ = tradetools.zero_None(data_dict, "direction", 'string')
            amount_ = tradetools.zero_None(data_dict, "amount")
            price_ = tradetools.zero_None(data_dict, "price")
            mark_price_ = tradetools.zero_None(data_dict, "mark_price")
            iv_ = tradetools.zero_None(data_dict, "iv")
            block_trade_leg_count_ = tradetools.zero_None(data_dict, "block_trade_leg_count")
            block_trade_id_ = tradetools.zero_None(data_dict, "block_trade_id", 'string')
            liquidation_ = tradetools.zero_None(data_dict, "liquidation", 'string')
            index_price_ = tradetools.zero_None(data_dict, "index_price")
            contracts_ = tradetools.zero_None(data_dict, "contracts")
            cursor_ = db_.cursor()
            sql_sentence_ = "INSERT INTO " + table_name_ + ("(instrument_name, trade_id, timestamp, trade_seq, "
                                                            "tick_direction, direction, amount, price, mark_price, "
                                                            "iv, block_trade_leg_count, block_trade_id, liquidation, "
                                                            "index_price, contracts) \
                         VALUES ('%s', '%s', %s, %s, %s, '%s', %s, %s, %s, %s, %s, '%s', '%s', %s, %s)") % \
                            (instrument_name_, trade_id_, timestamp_, trade_seq_, tick_direction_,
                             direction_, amount_, price_, mark_price_, iv_, block_trade_leg_count_,
                             block_trade_id_, liquidation_, index_price_, contracts_)
            try:
                cursor_.execute(sql_sentence_)
                db_.commit()
            except Exception as e:
                print("Error: unable to fetch data")
                db_.rollback()
            finally:
                cursor_.close()
    except Exception as eeeee:
        print(eeeee)
    finally:
        return has_more_, end_seq_


db_name = 'deribit'
instruments_list = []
try:
    table_name = 'option_files'
    db_connection_str = ('mysql+pymysql://root:' + setupmkt.MySQL_Password + '@' +
                         setupmkt.MySQL_host + ':' + str(setupmkt.MySQL_port) + '/' + db_name)
    db_connection = create_engine(db_connection_str)
    try:
        sql_sentence = "SELECT DISTINCT instrument_name FROM " + table_name
        df_sql = pd.read_sql(sql_sentence, con=db_connection)
        if df_sql.shape[0] >= 1:
            instruments_list = df_sql['instrument_name'].values
    except Exception as ee:
        print("Error: unable to read db")
    finally:
        db_connection.dispose()
except Exception as eee:
    print(eee)
instruments_num = len(instruments_list)
print(instruments_num)
db_mysql = pymysql.connect(host=setupmkt.MySQL_host, user='root', password=setupmkt.MySQL_Password, database=db_name,
                           port=setupmkt.MySQL_port)
table_name = 'option_trades'
for instrument_name in instruments_list:
    print(instrument_name)
    msg = {
        "method": "public/get_last_trades_by_instrument",
        "params": {
            "instrument_name": instrument_name,
            "start_seq": 1,
            # "count": 10000,
            "sorting": 'asc'
        },
        "jsonrpc": "2.0"
    }
    flag_more, end_seq = trades_sql(db_mysql, table_name, msg)
    while flag_more:
        msg = {
            "method": "public/get_last_trades_by_instrument",
            "params": {
                "instrument_name": instrument_name,
                "start_seq": end_seq + 1,
                # "count": 10000,
                "sorting": 'asc'
            },
            "jsonrpc": "2.0"
        }
        flag_more, end_seq = trades_sql(db_mysql, table_name, msg)
    print(end_seq)
db_mysql.close()
