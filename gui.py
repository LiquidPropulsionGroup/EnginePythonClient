# Data Retrieval
from flask import Flask, abort
import redis as red
import json, sys
# Data Display
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd
from datetime import datetime

# Create redis client for fetching data
# Use the Raspberry Pi IP, should be static
redis = red.Redis(host='192.168.137.10', port=6379)

# While loop control variable
Operation = True

# Redis Stream ID
stream_name = 'sensor_stream'

while Operation == True:
    # Grab the first item to establish the range for XREAD
    data = redis.xrange(stream_name, count=1)
    (label,data) = data[0]
    # Grab all the most recent XREAD data starting from the XRANGE
    data = redis.xread({ stream_name: f'{label.decode()}' }, block=0)
    (label, data) = data[0]
    # Iterate through the chunk of data
    for sensor_reading in data:
        (label, data) = sensor_reading
        print(label)
        print(data)
    # Grab the next set, starting after the most recent already read stream item
    data = redis.xrange(stream_name, min=f'{label.decode()}', count=1)
    (label,data) = data[0]
    data = redis.xread({ stream_name: f'{label.decode()}'}, block=0)