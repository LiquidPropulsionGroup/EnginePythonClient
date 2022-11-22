# HTTP Server host
import threading
from flask import Flask, abort
from flask_cors import CORS
# Data Retrieval
import redis
import json, sys, sqlite3
import re
import subprocess
import os
import csv

sensor_stream = 'sensor_stream'
sensor_fields = [
    "Timestamp",
    "PT_HE",
    "PT_Pneu",
    "PT_Purge",
    "PT_FUEL_PV",
    "PT_LOX_PV",
    "PT_FUEL_INJ",
    "PT_CHAM",
    "TC_FUEL_PV",
    "TC_LOX_PV",
    "TC_LOX_Valve_Main",
    "TC_WATER_In",
    "TC_WATER_Out",
    "TC_CHAM",
    "FT_Thrust"
]
event_stream = 'event_stream'
event_fields = [
    "Timestamp",
    "EVENT",
    "event",
    "FUEL_Press",
    "LOX_Press",
    "FUEL_Vent",
    "LOX_Vent",
    "MAIN",
    "FUEL_Purge",
    "LOX_Purge",
    "Water_Flow"
]
data_dir = '/data/Runs/'

# Flask app settings
app = Flask(__name__)
CORS(app)

# Host redis process location, contains data to collect
# Use the Raspberry Pi IP, should be static
# redisHost = red.Redis(host='192.168.137.10', port=6379)

# Client redis process location, contains replicaOf host
redisClient = redis.Redis(host='redis-db', port=6379)
print('Redis-db autosave intervals:')
redisClient.config_set('save', '300 1 15 100 5 1000')
print(redisClient.config_get('save'))
print(redisClient.config_set('appendonly','yes'))

def run_app():
    app.run(debug=False, threaded=True, host='0.0.0.0', port=3004)

def StopReplicate():
    # Stop duplicating the host
    # This remains the case until either database goes down, and is the default state
    redisClient.bgsave
    redisClient.bgrewriteaof
    redisClient.replicaof('NO', 'ONE')

def Replicate():
    # Duplicate the host onto the client
    # This remains the case until either database goes down
    redisClient.replicaof('192.168.137.10', '6379')
    # Save data that is there already
    redisClient.bgsave
    redisClient.bgrewriteaof

def Write():
    # Write everything in the db to csv's

    # Check what folders already exist
    directoryContains = os.listdir(data_dir)
    directoryCount = len(directoryContains)

    # Create a folder with a unique name (number one higher than amount of folders present alread)
    newFolderName = 'RunSave_' + str(directoryCount)
    os.mkdir(data_dir+newFolderName)
    print('Directory created')

    WriteSensorData(newFolderName)
    WriteEventData(newFolderName)

    return newFolderName

def WriteSensorData(newFolderName):
    # Dump sensor_stream into the directory
    sensor_data_first = redisClient.xrange(sensor_stream, count=1)
    (sensor_label_first, sensor_data_first) = sensor_data_first[0]
    print('Found data')
    sensor_data = redisClient.xread({ sensor_stream: f'{sensor_label_first.decode()}' }, block = 0)
    (sensor_label, sensor_data) = sensor_data[0]
    print('Data collated')

    # Writing to disk
    with open(data_dir + newFolderName + '/sensor_data.csv', mode='w') as sensor_file:
        # Establish the csv writing method
        sensor_writer = csv.DictWriter(sensor_file, fieldnames=sensor_fields)
        sensor_writer.writeheader()

        # Write the first row so it doesn't get missed
        [sensor_timestamp_item, multiInsertID] = re.split("-", sensor_label_first.decode())
        sensor_data_item = { key.decode(): val.decode() for key, val in sensor_data_first.items() }
        sensor_timestamp_item = { "Timestamp": sensor_timestamp_item }
        sensor_data_row = { **sensor_timestamp_item, **sensor_data_item }
        sensor_writer.writerow(sensor_data_row)

        for sensor_reading in sensor_data:
            # Separate data and timestamp
            (sensor_label_item, sensor_data_item) = sensor_reading
            # print(sensor_label_item)
            # print(sensor_data_item)
            # Split the timestamp
            [sensor_timestamp_item, multiInsertID] = re.split("-", sensor_label_item.decode())
            sensor_timestamp_item = { "Timestamp": sensor_timestamp_item }
            # Data is in a json/dict but is byte encoded, so strip it
            sensor_data_item = { key.decode(): val.decode() for key, val in sensor_data_item.items() }
            # Merge Timestamp and the data into one dict
            sensor_data_row = { **sensor_timestamp_item, **sensor_data_item }
            # Write the data to the .csv
            sensor_writer.writerow(sensor_data_row)
        print('SENSOR DATA SAVED')

def WriteEventData(newFolderName):
    # Dump sensor_stream into the directory
    event_data_first = redisClient.xrange(event_stream, count=1)
    (event_label_first, event_data_first) = event_data_first[0]
    # print(event_label_first)
    # print(event_data_first)
    print('Found data')
    event_data = redisClient.xread({ event_stream: f'{event_label_first.decode()}' }, block=0)
    (event_label, event_data) = event_data[0]
    # print(event_label)
    # print(event_data)
    print('Data collated')

    # Writing to disk
    with open(data_dir + newFolderName + '/event_data.csv', mode='w') as event_file:
        # Establish the csv writing method
        event_writer = csv.DictWriter(event_file, fieldnames=event_fields)
        event_writer.writeheader()

        # Write the first row so it doesn't get skipped
        [event_timestamp_item, multiInsertID] = re.split("-", event_label_first.decode())
        event_data_item = { key.decode(): val.decode() for key, val in event_data_first.items() }
        event_timestamp_item = { "Timestamp": event_timestamp_item }
        event_data_row = { **event_timestamp_item, **event_data_item }
        # print("=====================")
        # print(event_data_row)
        event_writer.writerow(event_data_row)

        # Step through the rest of the data
        for event_reading in event_data:
            # Separate data and timestamp
            # print(event_reading)
            (event_label_item, event_data_item) = event_reading
            # print(event_label_item)
            # print(event_data_item)
            # Split the timestamp
            [event_timestamp_item, multiInsertID] = re.split("-", event_label_item.decode())
            event_timestamp_item = { "Timestamp": event_timestamp_item }
            # print(event_timestamp_item)
            # Data is in a json/dict but is byte encoded, so strip it
            event_data_item = { key.decode(): val.decode() for key, val in event_data_item.items() }
            # print(event_data_item)
            # Merge Timestamp and the data into one dict
            event_data_row = { **event_timestamp_item, **event_data_item }
            # print(event_data_row)
            # Write the data to the .csv
            event_writer.writerow(event_data_row)
        print('EVENT DATA SAVED')

STORAGE_STATUS = False

@app.route('/serial/storage/<action>')
def ReplicationControl(action):
    global STORAGE_STATUS

    if action == 'START':
        print('REDIS REPLICATION START')
        Replicate()
        STORAGE_STATUS = True
        # Stop the storage loop
        return('REDIS REPLICATION STARTED')

    if action == 'CLOSE':
        print('REDIS REPLICATION STOP')
        StopReplicate()
        STORAGE_STATUS = False
        # Start the infinite loop of pulling and storing data
        return('REDIS REPLICATION STOPPED')

    if action == 'WRITE':
        print('PERSISTING REDIS-DB TO DISK')
        fileName = Write()
        return('REDIS PERSISTED TO ' + fileName)

    if action == 'STATUS':
        print('STATUS UPDATE REQUESTED')
        return str(STORAGE_STATUS)

    return(404)
    
if __name__ == "__main__":
    flaskApp_thread = threading.Thread(target=run_app)
    flaskApp_thread.start()