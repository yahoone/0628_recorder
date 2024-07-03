import setupmkt
import waytools
import pymysql
import time
import os


def sql_force(data_dict_):
    try:
        table_name = 'force_order'
        data_symbol = waytools.zero_None(data_dict_["o"], "s", 'string')
        data_timestamp = waytools.zero_None(data_dict_["o"], "T")
        data_direction = waytools.zero_None(data_dict_["o"], "S", 'string')
        data_quantity = float(waytools.zero_None(data_dict_["o"], "q"))
        data_price = float(waytools.zero_None(data_dict_["o"], "p"))
        data_average = float(waytools.zero_None(data_dict_["o"], "ap"))
        data_status = waytools.zero_None(data_dict_["o"], "X", 'string')
        data_type = waytools.zero_None(data_dict_["o"], "o", 'string')
        data_last = float(waytools.zero_None(data_dict_["o"], "l"))
        data_total = float(waytools.zero_None(data_dict_["o"], "z"))
        cursor_ = db.cursor()
        sql_sentence_ = "INSERT INTO " + table_name + ("(Symbol, TransactionTimestamp, Direction, Quantity, "
                                                       "Price, Average, Status, OrderType, LastVolume, TotalVolume) \
                     VALUES ('%s', %s, '%s', %s, %s, %s, '%s', '%s', %s, %s)") % \
                        (data_symbol, data_timestamp, data_direction, data_quantity, data_price, data_average,
                         data_status, data_type, data_last, data_total)
        try:
            cursor_.execute(sql_sentence_)
            db.commit()
        except Exception as e:
            print("error in fetching data: "+str(e))
            db.rollback()
        finally:
            cursor_.close()
    except Exception as error_handle:
        print('error in running sql: '+str(error_handle))


market_pair = 'btcusdt'
market_key = 'usdt'
while True:
    db = pymysql.connect(host=setupmkt.MySQL_host, user='root', password=setupmkt.MySQL_Password, database='binance',
                         port=setupmkt.MySQL_port)
    try:
        time.sleep(1)
        dict_timestamps = waytools.load_json('forceOrders/' + market_pair + '_' + market_key + '_timestamps.json')
        list_timestamps = dict_timestamps['timestamps']
        for timestamp_ in list_timestamps:
            file_path_ = 'forceOrders/' + market_pair+'_'+market_key+'_'+str(timestamp_)+'.json'
            if os.path.isfile(file_path_):
                data_dict = waytools.load_json(file_path_)
                print(data_dict)
                sql_force(data_dict)
                waytools.delete_file(file_path_)
    except Exception as error_json:
        print('error in reading json: '+str(error_json))
    finally:
        db.close()
