from flask import Flask, abort
import redis as red
import json, sys

####* User defined variables START *####
try:
    sys.argv[1]
except IndexError:
    stream_name = 'sensor_stream' # defult value
else:
    stream_name = sys.argv[1]
####! User defined variables END !####

# Creating redis client
redis = red.Redis(host='192.168.137.10', port=6379)
print(redis)
print('Generating response_class')
data = redis.xrange(stream_name, count=1)
print(data)
(label, data) = data[0]
data = redis.xread({ stream_name: f'{label.decode()}' }, block=0)
print(data)
(label, data) = data[0]
for sensor_reading in data:
    print('Sensor reading is:')
    print(sensor_reading)
    (label, reading) = sensor_reading
    print('Label and reading:')
    print(label)
    print(reading)
    # Transform redis stream json object to bytes
    string_label = label.decode('UTF-8')
    string_reading = { key.decode(): val.decode() for key, val in reading.items() }
    string_reading = json.dumps(string_reading)
    print('Strings are:')
    print(string_label)
    print(string_reading)
    #yield string_label + string_reading
    print('======================NEW DATA GRAB=======================')