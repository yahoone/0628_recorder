import setupmkt
import waytools
import websockets
import json
import asyncio


async def subscribe_trades(market_pair, market_key='usdt'):
    channel_str = 'forceOrder'
    try:
        event_str = setupmkt.dict_channels[channel_str]
        list_params = [market_pair + '@' + channel_str]
        list_timestamps = []
        msg_ = {"method": "SUBSCRIBE", "id": setupmkt.get_timestamp(), "params": list_params}
        async with websockets.connect(setupmkt.dict_streams[market_key]+'/ws') as websocket:
            await websocket.send(json.dumps(msg_))
            while websocket.open:
                response = await websocket.recv()
                print(response)
                try:
                    dict_res = json.loads(response)
                    if "e" in dict_res.keys():
                        if dict_res["e"] == event_str:
                            timestamp_ = dict_res['E']
                            waytools.json_dump('forceOrders/'+market_pair+'_'+market_key+'_'+str(timestamp_)+'.json',
                                               dict_res)
                            list_timestamps.append(timestamp_)
                            list_timestamps.sort()
                            if len(list_timestamps) > 5:
                                list_timestamps.pop(0)
                            waytools.json_dump('forceOrders/' + market_pair + '_' + market_key + '_timestamps.json',
                                               {'timestamps': list_timestamps})
                except Exception as solve_error:
                    print(solve_error)
    except Exception as subscribe_error:
        print(subscribe_error)


loop_ = asyncio.new_event_loop()
asyncio.set_event_loop(loop_)
try:
    loop_.run_until_complete(subscribe_trades(market_pair='btcusdt'))
except Exception as eeeee:
    print(eeeee)
finally:
    loop_.close()
