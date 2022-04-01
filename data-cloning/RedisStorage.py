# HTTP Server host
from flask import Flask, abort
# Data Retrieval
import redis as red
from importlib.metadata import version

# Flask app settings
app = Flask(__name__)

# Host redis process location, contains data to collect
# Use the Raspberry Pi IP, should be static
redisHost = red.Redis(host='192.168.137.10', port=6379)

# Client redis process location, contains replicaOf host
redisClient = red.Redis(host='redis-db', port=6379)

def StopReplicate():
    # Stop duplicating the host
    # This remains the case until either database goes down, and is the default state
    redisClient.replicaof('NO ONE')

def Replicate():
    # Duplicate the host onto the client
    # This remains the case until either database goes down
    redisClient.replicaof('192.168.137.10', '6379')

@app.route('/serial/storage/<action>')
def ReplicationControl(action):
    if action == 'START':
        print('REDIS REPLICATION START')
        Replicate()
        return('REDIS REPLICATION STARTED')

    if action == 'CLOSE':
        print('REDIS REPLICATION STOP')
        StopReplicate()
        return('REDIS REPLICATION STOPPED')

    return(404)
    
if __name__ == "__main__":
    app.run(debug=False, threaded=True, host='0.0.0.0', port=3004)