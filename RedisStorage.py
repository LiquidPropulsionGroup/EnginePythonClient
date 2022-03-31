# Data Retrieval
from flask import Flask, abort
import redis as red
import json, sys
import itertools

# Host redis process location, contains data to collect
# Use the Raspberry Pi IP, should be static
redisHost = red.Redis(host='192.168.137.10', port=6379)

# Client redis process location, contains replicaOf host
redisClient = red.Redis(host='localhost', port=6379)

