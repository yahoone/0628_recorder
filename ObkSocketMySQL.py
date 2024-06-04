import asyncio
import websockets
import ssl
import certifi
import requests
import json
from datetime import datetime
import pymysql
import setupmkt
import obktools


ssl_context = ssl.create_default_context()
ssl_context.load_verify_locations(certifi.where())

msg = {
    "method": "public/get_instruments",
    "params": {
        "currency": "BTC",
        "kind": "option"
    },
    "jsonrpc": "2.0"
}
response = requests.post(setupmkt.url, json=msg)
data = response.json()
instruments_list = []
for j in range(len(data['result'])):
    instruments_list.append(data['result'][j]['instrument_name'])
instruments_num = len(instruments_list)
msgs_dict = {}
asks_dict = {}
bids_dict = {}
for instrument_ in instruments_list:
    msgs_dict[instrument_] = {
        "method": "public/get_order_book",
        "params": {
            "instrument_name": instrument_,
            "depth": 5
        },
        "jsonrpc": "2.0"
    }
    asks_dict[instrument_] = [[0.0, 0.0], [0.0, 0.0], [0.0, 0.0], [0.0, 0.0], [0.0, 0.0]]
    bids_dict[instrument_] = [[0.0, 0.0], [0.0, 0.0], [0.0, 0.0], [0.0, 0.0], [0.0, 0.0]]
datetime_error = []
msg_error = []
data_error = []
table_name = 'option_orderbooks'
db = pymysql.connect(host='192.168.50.60', user='root', password=setupmkt.MySQL_Password, database='deribit', port=13006)
cursor0 = db.cursor()
cursor0.execute("SELECT * FROM "+table_name)
res = cursor0.fetchall()
print(len(res))
cursor0.close()


async def db_call_api(instruments_):
    while True:
        print(datetime.now())
        try:
            async with (websockets.connect('wss://www.deribit.com/ws/api/v2', ssl=ssl_context) as websocket):
                for instrument_j in instruments_:
                    msg_ = msgs_dict[instrument_j]
                    await websocket.send(json.dumps(msg_))
                    res_ = await websocket.recv()
                    data_ = json.loads(res_)
                    data_dict = data_["result"]
                    asks_list = data_dict["asks"]
                    bids_list = data_dict["bids"]
                    if obktools.obk_differ(asks_list, asks_dict[instrument_j]) or obktools.obk_differ(
                            bids_list, bids_dict[instrument_j]):
                        print('The instrument ' + data_dict["instrument_name"] + ' has new order-books.')
                        cursor = db.cursor()
                        asks_dict[instrument_j] = asks_list
                        bids_dict[instrument_j] = bids_list
                        instrument_name_ = data_dict["instrument_name"]
                        timestamp_ = data_dict["timestamp"]
                        ask_iv_ = obktools.zero_None(data_dict, "ask_iv")
                        asks1price_ = obktools.obk_split(asks_list, 1, 'price')
                        asks2price_ = obktools.obk_split(asks_list, 2, 'price')
                        asks3price_ = obktools.obk_split(asks_list, 3, 'price')
                        asks4price_ = obktools.obk_split(asks_list, 4, 'price')
                        asks5price_ = obktools.obk_split(asks_list, 5, 'price')
                        asks1size_ = obktools.obk_split(asks_list, 1, 'size')
                        asks2size_ = obktools.obk_split(asks_list, 2, 'size')
                        asks3size_ = obktools.obk_split(asks_list, 3, 'size')
                        asks4size_ = obktools.obk_split(asks_list, 4, 'size')
                        asks5size_ = obktools.obk_split(asks_list, 5, 'size')
                        best_ask_amount_ = obktools.zero_None(data_dict, "best_ask_amount")
                        best_ask_price_ = obktools.zero_None(data_dict, "best_ask_price")
                        best_bid_amount_ = obktools.zero_None(data_dict, "best_bid_amount")
                        best_bid_price_ = obktools.zero_None(data_dict, "best_bid_price")
                        bid_iv_ = obktools.zero_None(data_dict, "bid_iv")
                        bids1price_ = obktools.obk_split(bids_list, 1, 'price')
                        bids2price_ = obktools.obk_split(bids_list, 2, 'price')
                        bids3price_ = obktools.obk_split(bids_list, 3, 'price')
                        bids4price_ = obktools.obk_split(bids_list, 4, 'price')
                        bids5price_ = obktools.obk_split(bids_list, 5, 'price')
                        bids1size_ = obktools.obk_split(bids_list, 1, 'size')
                        bids2size_ = obktools.obk_split(bids_list, 2, 'size')
                        bids3size_ = obktools.obk_split(bids_list, 3, 'size')
                        bids4size_ = obktools.obk_split(bids_list, 4, 'size')
                        bids5size_ = obktools.obk_split(bids_list, 5, 'size')
                        estimated_delivery_price_ = obktools.zero_None(data_dict, "estimated_delivery_price")
                        greeks_delta_ = obktools.zero_None(data_dict["greeks"], "delta")
                        greeks_gamma_ = obktools.zero_None(data_dict["greeks"], "gamma")
                        greeks_rho_ = obktools.zero_None(data_dict["greeks"], "rho")
                        greeks_theta_ = obktools.zero_None(data_dict["greeks"], "theta")
                        greeks_vega_ = obktools.zero_None(data_dict["greeks"], "vega")
                        index_price_ = obktools.zero_None(data_dict, "index_price")
                        interest_rate_ = obktools.zero_None(data_dict, "interest_rate")
                        last_price_ = obktools.zero_None(data_dict, "last_price")
                        mark_iv_ = obktools.zero_None(data_dict, "mark_iv")
                        mark_price_ = obktools.zero_None(data_dict, "mark_price")
                        max_price_ = obktools.zero_None(data_dict, "max_price")
                        min_price_ = obktools.zero_None(data_dict, "min_price")
                        open_interest_ = obktools.zero_None(data_dict, "open_interest")
                        settlement_price_ = obktools.zero_None(data_dict, "settlement_price")
                        state_ = obktools.zero_None(data_dict, "state")
                        stats_high_ = obktools.zero_None(data_dict["stats"], "high")
                        stats_low_ = obktools.zero_None(data_dict["stats"], "low")
                        stats_price_change_ = obktools.zero_None(data_dict["stats"], "price_change")
                        stats_volume_ = obktools.zero_None(data_dict["stats"], "volume")
                        stats_volume_usd_ = obktools.zero_None(data_dict["stats"], "volume_usd")
                        underlying_price_ = obktools.zero_None(data_dict, "underlying_price")
                        underlying_index_ = obktools.zero_None(data_dict, "underlying_index")
                        sql_sentence = "INSERT INTO " + table_name + ("(instrument_name, timestamp, ask_iv, "
                                                                      "asks1price, asks1size, asks2price, asks2size, "
                                                                      "asks3price, asks3size, asks4price, asks4size, "
                                                                      "asks5price, asks5size, best_ask_amount, "
                                                                      "best_ask_price, best_bid_amount, "
                                                                      "best_bid_price, bid_iv, bids1price, bids1size, "
                                                                      "bids2price, bids2size, bids3price, bids3size, "
                                                                      "bids4price, bids4size, bids5price, bids5size, "
                                                                      "estimated_delivery_price, greeks_delta, "
                                                                      "greeks_gamma, greeks_rho, greeks_theta, "
                                                                      "greeks_vega, index_price, interest_rate, "
                                                                      "last_price, mark_iv, mark_price, max_price, "
                                                                      "min_price, open_interest, settlement_price, "
                                                                      "state, stats_high, stats_low, "
                                                                      "stats_price_change, stats_volume, "
                                                                      "stats_volume_usd, underlying_price, "
                                                                      "underlying_index) \
                                     VALUES ('%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                                                      "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                                                      "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, "
                                                                      "%s, '%s', %s, %s, %s, %s, %s, %s, '%s')") % \
                                       (instrument_name_, timestamp_, ask_iv_, asks1price_, asks1size_, asks2price_,
                                        asks2size_, asks3price_, asks3size_, asks4price_, asks4size_, asks5price_,
                                        asks5size_, best_ask_amount_, best_ask_price_, best_bid_amount_,
                                        best_bid_price_, bid_iv_, bids1price_, bids1size_, bids2price_, bids2size_,
                                        bids3price_, bids3size_, bids4price_, bids4size_, bids5price_, bids5size_,
                                        estimated_delivery_price_, greeks_delta_, greeks_gamma_, greeks_rho_,
                                        greeks_theta_, greeks_vega_, index_price_, interest_rate_, last_price_,
                                        mark_iv_, mark_price_, max_price_, min_price_, open_interest_,
                                        settlement_price_, state_, stats_high_, stats_low_, stats_price_change_,
                                        stats_volume_, stats_volume_usd_, underlying_price_, underlying_index_)
                        try:
                            cursor.execute(sql_sentence)
                            db.commit()
                        except Exception as e:
                            print("Error: unable to fetch data")
                            db.rollback()
                        finally:
                            cursor.close()
        except Exception as eee:
            print("ERROR: ")
            msg_error.append(eee)
            print(eee)
        finally:
            print(datetime.now())
            print('Now the recorded errors are:')
            print(msg_error)


loop_ = asyncio.get_event_loop()
try:
    loop_.run_until_complete(db_call_api(instruments_list))
except Exception as eeeee:
    print(eeeee)
finally:
    loop_.close()
    db.close()
