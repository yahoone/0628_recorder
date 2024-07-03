import time


dict_urls = {'spot': "https://api.binance.com",
             'usdt': "https://fapi.binance.com",
             'coin': "https://dapi.binance.com",
             'pm': "https://papi.binance.com"}
dict_endpoints = {'spot': "/api/v3/",
                  'usdt': "/fapi/v1/",
                  'coin': "/dapi/v1/",
                  'pm': "/papi/v1/"}
dict_streams = {'spot': "wss://stream.binance.com:9443",
                'usdt': "wss://fstream.binance.com",
                'coin': "wss://dstream.binance.com",
                'pm': "wss://fstream.binance.com/pm"}
dict_sockets = {'spot': "wss://ws-api.binance.com:443/ws-api/v3",
                'usdt': "wss://ws-fapi.binance.com/ws-fapi/v1"}
dict_markets = {'usdt': "um", 'coin': "cm", 'margin': "margin"}
dict_channels = {'aggTrade': 'aggTrade', 'markPrice': 'markPriceUpdate', 'miniTicker': '24hrMiniTicker',
                 'ticker': '24hrTicker', 'bookTicker': 'bookTicker', 'forceOrder': 'forceOrder'}
MySQL_host = '192.168.50.60'
MySQL_port = 13006
MySQL_Password = 'wEA_624AycKX6Pp7'

def get_timestamp():
    return int(time.time() * 1000)
