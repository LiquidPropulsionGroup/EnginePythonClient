# Data Retrieval
from flask import Flask, abort
import redis as red
import json, sys
import itertools
# Websocket
import asyncio
import websockets as ws
import re
import time
# For Debugging
import logging

# logger = logging.getLogger('websockets')
# logger.setLevel(logging.DEBUG)
# logger.addHandler(logging.StreamHandler())
# logging.basicConfig(filename="G:\CSUS\LPG\debug\log.txt")

#test

app = Flask(__name__)

# Websockets Server
async def main():
    print("Main")
    async with ws.serve(producer_handler, port=8765):
        print("ws")
        try:
            print("Trying to run forever")
            await asyncio.Future()  # run forever
        except ws.exceptions.ConnectionClosed:
            print("Connection close")
            pass
        except asyncio.exceptions.CancelledError:
            print("Connection cancelled?")
            pass

async def producer_handler(websocket, path):
    # Produce sensor data for the websocket
    # Need to start from the beginning for each websocket client
    Loop_starter = 0
    print("sensor producer start")
    baseTime = time.time()
    ping = True
    while True:
        # Package sensor and valve data
        # print("Looping...")
        try:
            # From the sensor producer
            sensor_message = await sensor_producer(Loop_starter)
            # And the valve producer
            # valve_message = await valve_producer(Loop_starter)
            # After the initial loop, change the condition
            Loop_starter = 1
        except ws.exceptions.ConnectionClosed:
            print("Connection closed1")
            pass
        except asyncio.exceptions.CancelledError:
            print("Connection cancelled?")
            pass

        # Then send it through websocket to client
        # Sensor:
        try:
            # count = count + 1
            # print("Item " + str(count) + ":")
            # print(item)
            # print(json.dumps({'message': message}))
            if not sensor_message:
                # Check if client is still alive
                # print("Pinging client")
                await websocket.ping()
            else:
                # Send the messages
                await websocket.send(json.dumps({'message': sensor_message}))
                # Don't overload the websocket
                await asyncio.sleep(.1)
                # Confirm reception of the data frame
                response = await websocket.recv()
                print(response)
        except ws.exceptions.ConnectionClosed:
            print("Connection closed2")
            await websocket.close()
            return
        
        # Valve:
        # try:
        #     # count = count + 1
        #     # print("Item " + str(count) + ":")
        #     # print(item)
        #     # print(json.dumps({'message': message}))
        #     if not valve_message:
        #         # Check if client is still alive
        #         # print("Pinging client")
        #         await websocket.ping()
        #     else:
        #         # Send the messages   
        #         await websocket.send(json.dumps({'message': valve_message}))
        #         print("MESSAGE SENT")
        #         await asyncio.sleep(.1)
        # except ws.exceptions.ConnectionClosed:
        #     print("Connection closed")
        #     await websocket.close()
        #     return

        currentTime = time.time()
        elapsed = currentTime - baseTime
        if elapsed >= 5:
            # Request update from client
            await websocket.send(json.dumps({'message': 'PING'}))
            try:
                await asyncio.wait_for(websocket.recv(), timeout = 5.0)
            except asyncio.TimeoutError:
                print('CLIENT TIMED OUT')
                await websocket.close()
                break
            baseTime = time.time()





# Loop for grabbing information from the Pi-hosted redis sensor_stream
async def sensor_producer(Loop_starter):
    # print("=============================")
    global sensor_data
    global sensor_label

    # When starting the loop, reach all the way back to the start of the stream and package that data

    try: 
        if Loop_starter == 0:
            # Grab the first item to establish the range for XREAD
            sensor_data = redis.xrange(sensor_stream_name, count=1)
            (sensor_label,sensor_data) = sensor_data[0]
            # Grab all the most recent XREAD data starting from the XRANGE
            sensor_data = redis.xread({ sensor_stream_name: f'{sensor_label.decode()}' }, block=0)
            (sensor_label,sensor_data) = sensor_data[0]
        else:
            # Grab the next set of data, starting after the most recent already read stream item
            sensor_data = redis.xrange(sensor_stream_name, min=f'{sensor_label.decode()}', count=1)
            (sensor_label,sensor_data) = sensor_data[0]
            sensor_data = redis.xread({ sensor_stream_name: f'{sensor_label.decode()}'}, block=1)
            (sensor_label,sensor_data) = sensor_data[0]
    except IndexError:
        # print("INDEX ERROR")
        return []
    except NameError:
        print("Didn't initally loop")
        return []
    except ws.exceptions.ConnectionClosed:
        print("Connection Closed")
        pass
    except asyncio.exceptions.CancelledError:
        print("Connection cancelled?")
        pass

    # Iterate through the chunk of 'new' data
    data_package = []
    # Grab only every nth element for packaging
    n = 1
    for sensor_reading in itertools.islice(sensor_data,None,None,n):
        (sensor_label, sensor_data) = sensor_reading
        sensor_timestamp = re.split("-", sensor_label.decode())
        # print(sensor_reading)
        data_buffer = {'Timestamp': f"{sensor_timestamp[0]}",
                            'PT_HE': f"{sensor_data[b'PT_HE'].decode()}",
                            # 'PT_Purge': f"{sensor_data[b'PT_Purge'].decode()}",
                            'PT_Pneu': f"{sensor_data[b'PT_Pneu'].decode()}",
                            'PT_FUEL_PV': f"{sensor_data[b'PT_FUEL_PV'].decode()}",
                            'PT_LOX_PV': f"{sensor_data[b'PT_LOX_PV'].decode()}",
                            'PT_FUEL_INJ': f"{sensor_data[b'PT_FUEL_INJ'].decode()}",
                            'PT_CHAM': f"{sensor_data[b'PT_CHAM'].decode()}",
                            'TC_FUEL_PV': f"{sensor_data[b'TC_FUEL_PV'].decode()}",
                            'TC_LOX_PV': f"{sensor_data[b'TC_LOX_PV'].decode()}",
                            'TC_LOX_Valve_Main': f"{sensor_data[b'TC_LOX_Valve_Main'].decode()}",
                            'TC_WATER_In': f"{sensor_data[b'TC_WATER_In'].decode()}",
                            'TC_WATER_Out': f"{sensor_data[b'TC_WATER_Out'].decode()}",
                            'TC_CHAM': f"{sensor_data[b'TC_CHAM'].decode()}",
                            #'RC_LOX_Level': f"{sensor_data[b'RC_LOX_Level'].decode()}",
                            'FT_Thrust': f"{sensor_data[b'FT_Thrust'].decode()}"
                            }
        # print(data_buffer)
        data_package.append(data_buffer)
        #print(label)
        #print(data)

    # Pipe to websocket
    print("Websocketed Sensor Data")
    print("========================")
    # print(data_package)
    # await websocket.send('test')
    return data_package

async def valve_producer(Loop_starter):
    # print("=============================")
    global valve_data
    global valve_label
    try:
        if Loop_starter == 0:
            # Grab the first item to establish the range for XREAD
            valve_data = redis.xrange(valve_stream_name, count=1)
            (valve_label,valve_data) = valve_data[0]
            # Grab all the most recent XREAD data starting from the XRANGE
            valve_data = redis.xread({ valve_stream_name: f'{valve_label.decode()}' }, block=0)
            # print(valve_data)
            (valve_label,valve_data) = valve_data[0]
        else:
            # Grab the next set of data, starting after the most recent already read stream item
            valve_data = redis.xrange(valve_stream_name, min=f'{valve_label.decode()}', count=1)
            (valve_label,valve_data) = valve_data[0]
            valve_data = redis.xread({ valve_stream_name: f'{valve_label.decode()}'}, block=1)
            (valve_label, valve_data) = valve_data[0]
            
    except IndexError:
        # print("Index error")
        return []
    except NameError:
        print("Didn't initally loop")
        return []
    except ws.exceptions.ConnectionClosed:
        pass
    except asyncio.exceptions.CancelledError:
        pass

    # Iterate through the chunk of 'new' data
    data_package = []
    for valve_reading in valve_data:
        (valve_label, valve_data) = valve_reading
        valve_timestamp = re.split("-", valve_label.decode())
        data_buffer = {'Timestamp': f"{valve_timestamp[0]}",
                            'FUEL_Press': f"{valve_data[b'FUEL_Press'].decode()}",
                            'LOX_Press': f"{valve_data[b'LOX_Press'].decode()}",
                            'FUEL_Vent': f"{valve_data[b'FUEL_Vent'].decode()}",
                            'LOX_Vent': f"{valve_data[b'LOX_Vent'].decode()}",
                            'MAIN': f"{valve_data[b'MAIN'].decode()}",
                            'FUEL_Purge': f"{valve_data[b'FUEL_Purge'].decode()}",
                            'LOX_Purge': f"{valve_data[b'LOX_Purge'].decode()}"
                            }
        # print(data_buffer)
        data_package.append(data_buffer)
        #print(label)
        #print(data)

    # Pipe to websocket
    print("Websocketed Valve Update")
    print("========================")
    print(data_package)
    # await websocket.send('test')
    return data_package


# Create redis client for fetching data
# Use the Raspberry Pi IP, should be static
redis = red.Redis(host='localhost', port=6379)

# While loop control variable
Operation = True

# Redis Stream ID
sensor_stream_name = 'sensor_stream'
valve_stream_name = 'valve_stream'

# Global var
global sensor_data
sensor_data = ''
global sensor_label

global valve_data
valve_data = ''
global valve_label

# Run the websocket server
print("Run server")
asyncio.run(main())

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3003)   