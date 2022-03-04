# Data Retrieval
from flask import Flask, abort
import redis as red
import json, sys
import itertools
# Websocket
import asyncio
import websockets as ws
import re
# For Debugging
import logging

# logger = logging.getLogger('websockets')
# logger.setLevel(logging.DEBUG)
# logger.addHandler(logging.StreamHandler())
# logging.basicConfig(filename="G:\CSUS\LPG\debug\log.txt")


# Websockets Server
async def main():
    print("Main")
    async with ws.serve(producer_handler, "localhost", 8765):
        print("ws")
        try:
            await asyncio.Future()  # run forever
        except ws.exceptions.ConnectionClosed:
            pass
        except asyncio.exceptions.CancelledError:
            pass
        # except KeyboardInterrupt:
        #     pass

async def producer_handler(websocket, path):
    # Produce sensor data for the websocket
    # count = 0
    Loop_starter = 0
    print("sensor producer start")
    while True:
        try:
            sensor_message = await sensor_producer(Loop_starter)
            valve_message = await valve_producer(Loop_starter)
            Loop_starter = 1
        except ws.exceptions.ConnectionClosed:
            pass
        except asyncio.exceptions.CancelledError:
            pass
        # except KeyboardInterrupt:
        #     pass

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
                # Send the message
                await websocket.send(json.dumps({'message': sensor_message}))
                await asyncio.sleep(.1)
        except ws.exceptions.ConnectionClosed:
            print("Connection closed")
            await websocket.close()
            return
        
        try:
            # count = count + 1
            # print("Item " + str(count) + ":")
            # print(item)
            # print(json.dumps({'message': message}))
            if not valve_message:
                # Check if client is still alive
                # print("Pinging client")
                await websocket.ping()
            else:
                # Send the message
                
                await websocket.send(json.dumps({'message': valve_message}))
                print("MESSAGE SENT")
                await asyncio.sleep(.1)
        except ws.exceptions.ConnectionClosed:
            print("Connection closed")
            await websocket.close()
            return

        # very important line :)!
        # await asyncio.sleep(2)
        # await asyncio.sleep(0.05)

# Loop for grabbing information from the Pi-hosted redis sensor_stream
async def sensor_producer(Loop_starter):
    # print("=============================")
    global sensor_data
    global sensor_label
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
        pass
    except asyncio.exceptions.CancelledError:
        pass

    # Iterate through the chunk of 'new' data
    data_package = []
    # Grab only every nth element for packaging
    n = 10
    for sensor_reading in itertools.islice(sensor_data,None,None,n):
        (sensor_label, sensor_data) = sensor_reading
        sensor_timestamp = re.split("-", sensor_label.decode())
        # print(sensor_reading)
        data_buffer = {'Timestamp': f"{sensor_timestamp[0]}",
                            'PT_HE': f"{sensor_data[b'PT_HE'].decode()}",
                            'PT_Purge': f"{sensor_data[b'PT_Purge'].decode()}",
                            'PT_Pneu': f"{sensor_data[b'PT_Pneu'].decode()}",
                            'PT_FUEL_PV': f"{sensor_data[b'PT_FUEL_PV'].decode()}",
                            'PT_LOX_PV': f"{sensor_data[b'PT_LOX_PV'].decode()}",
                            #'PT_FUEL_INJ': f"{sensor_data[b'PT_FUEL_INJ'].decode()}",
                            'PT_CHAM': f"{sensor_data[b'PT_CHAM'].decode()}",
                            'TC_FUEL_PV': f"{sensor_data[b'TC_FUEL_PV'].decode()}",
                            'TC_LOX_PV': f"{sensor_data[b'TC_LOX_PV'].decode()}",
                            'TC_LOX_Valve_Main': f"{sensor_data[b'TC_LOX_Valve_Main'].decode()}",
                            'TC_WATER_In': f"{sensor_data[b'TC_WATER_In'].decode()}",
                            'TC_WATER_Out': f"{sensor_data[b'TC_WATER_Out'].decode()}",
                            'TC_CHAM': f"{sensor_data[b'TC_CHAM'].decode()}",
                            #'RC_LOX_Level': f"{sensor_data[b'RC_LOX_Level'].decode()}",
                            'FT_Thrust': f"{sensor_data[b'FT_Thrust'].decode()}",
                            'FL_WATER': f"{sensor_data[b'FL_WATER'].decode()}"
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
redis = red.Redis(host='192.168.137.10', port=6379)

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
