# Data Retrieval
from flask import Flask, abort
import redis as red
import json, sys
# Websocket
import asyncio
import websockets as ws

# Websockets Server
async def main():
    async with ws.serve(producer_handler, "localhost", 8765):
        await asyncio.Future()  # run forever

async def producer_handler(websocket, path):
    count = 0
    last_message = 0
    print("producer start")
    await asyncio.sleep(0.05)
    while True:
        
        message = await producer(last_message)
        try:
            count = count + 1
            print("Item " + str(count) + ":")
            print(item)
            await websocket.send(json.dumps({'message': message}))
            # await asyncio.sleep(.1)
            if not message:
                # Check if client is still alive
                print("Pinging client")
                await websocket.ping()
        except ws.exceptions.ConnectionClosed:
            print("Connection closed")
            await websocket.close()
            return
        
        # very important line :)!
        await asyncio.sleep(2)

# Loop for grabbing information from the Pi-hosted redis stream
async def producer(last_message):

    global data
    global label

    if last_message == 0:
        # Grab the first item to establish the range for XREAD
        data = redis.xrange(stream_name, count=1)
        (label,data) = data[0]
        # Grab all the most recent XREAD data starting from the XRANGE
        data = redis.xread({ stream_name: f'{label.decode()}' }, block=0)
        (label,data) = data[0]
    else:
        # Grab the next set of data, starting after the most recent already read stream item
        data = redis.xrange(stream_name, min=f'{label.decode()}', count=1)
        (label,data) = data[0]
        data = redis.xread({ stream_name: f'{label.decode()}'}, block=1)
        try:
            (label,data) = data[0]
        except IndexError:
            print("Index error")
            return []

    # Iterate through the chunk of 'new' data
    data_package = []
    for sensor_reading in data:
        (label, data) = sensor_reading
        print(sensor_reading)
        data_buffer = json.dumps({"Timestamp": f"{label.decode()}", "PT_HE": f"{data[b'PT_HE'].decode()}", "PT_Purge": f"{data[b'PT_Purge'].decode()}", "PT_Pneu": f"{data[b'PT_Pneu'].decode()}", "PT_FUEL_PV": f"{data[b'PT_FUEL_PV'].decode()}", "PT_LOX_PV": f"{data[b'PT_LOX_PV'].decode()}", "PT_FUEL_INJ": f"{data[b'PT_FUEL_INJ'].decode()}", "PT_CHAM": f"{data[b'PT_CHAM'].decode()}", "TC_FUEL_PV": f"{data[b'TC_FUEL_PV'].decode()}", "PT_LOX_PV": f"{data[b'TC_LOX_PV'].decode()}", "TC_LOX_Valve_Main": f"{data[b'TC_LOX_Valve_Main'].decode()}", "RC_LOX_Level": f"{data[b'RC_LOX_Level'].decode()}", "FT_Thrust": f"{data[b'FT_Thrust'].decode()}"})
        #print(data_buffer)
        data_package = [*data_package, data_buffer]
        #print(label)
        #print(data)

    # Pipe to websocket
    print("Websocketed")
    # await websocket.send('test')
    return data_package

# Create redis client for fetching data
# Use the Raspberry Pi IP, should be static
redis = red.Redis(host='192.168.137.10', port=6379)

# While loop control variable
Operation = True

# Redis Stream ID
stream_name = 'sensor_stream'

# Global var
global data
data = ''
global label

# Run the websocket server
asyncio.run(main())