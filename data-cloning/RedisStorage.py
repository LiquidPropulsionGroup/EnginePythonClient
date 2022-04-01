# Data Retrieval
import redis as red
import json, sys
import itertools
from importlib.metadata import version
print("Redis Version")
print(version('redis'))

# Host redis process location, contains data to collect
# Use the Raspberry Pi IP, should be static
redisHost = red.Redis(host='192.168.137.10', port=6379)

# Client redis process location, contains replicaOf host
redisClient = red.Redis(host='redis-db', port=6379)

print("Is command callable?")
command = getattr(redisClient, "replicaof", None)
print(callable(command))

# Duplicate the host onto the client
# redisClient.xrange('192.168.137.10', '6379')

print("container up", flush=True)