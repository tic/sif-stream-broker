# Import necessary libraries
import paho.mqtt.client as mqtt
from datetime import datetime
import json

# Import database connector and create a blank
# connection object
import db
db_connection = None

# Load env variables and the function to read them
from os import getenv as os_getenv
from dotenv import load_dotenv
load_dotenv()


# Executes when a message arrives in the ingest stream
def on_message_receive(client, userdata, message):
    message = message.payload.decode("utf-8")
    parsed = json.loads(message)
    print(message)
    db.insert_ir_message(db_connection, parsed['app_id'], parsed['data'])


# Create a mqtt client with a unique time-based name.
# Note: uniqueness is guaranteed provided multiple
#       clients are not created at the exact same
#       instance in time.
def spawn_client():
    # Create a client instance with a unique name
    ctime = datetime.utcnow().timestamp()
    return mqtt.Client(f'StreamProcessor.{ctime}')    


# Given a client, configure it for receiving messages
# on the ingest stream. After this function returns,
# the specified client will be elligible to receive
# messages from the ingest stream.
def setup_client(client):
    # Connect to the ingest stream host
    if os_getenv('ENV') == 'DEVELOPMENT':
        client.connect('localhost')
    else:
        client.connect(os_getenv('INGEST_STREAM'))

    # Initiate the client's event loop
    client.loop_start()

    # Subscribe to the ingest data stream
    client.subscribe('ingest/stream')

    # Register the on_message handler
    client.on_message = on_message_receive


# Properly shuts down a client instance by ending its
# event loop and disconnecting it from the ingest
# stream.
def cleanup_client(client):
    client.loop_stop()
    client.disconnect()


# Generate a single client and wait. After the loop
# is stopped by a user, shut down the client and
# terminate.
if __name__ == '__main__':
    # Import libraries
    from time import sleep

    # Spawn a single consumer client
    client = spawn_client()

    db_connection = db.create_connection(
        os_getenv('TS_USER'), 
        os_getenv('TS_PASSWD'), 
        os_getenv('TS_HOST'), 
        os_getenv('TS_PORT'),
        os_getenv('TS_DATABASE')
    )

    if not db_connection:
        raise 'Failed to connect to database'

    # Prepare the client to receive messages
    setup_client(client)

    # Loop indefinitely until stopped by a user
    try:
        while True:
            sleep(60)
    except KeyboardException:
        pass

    # Clean up the client before shutting down
    cleanup_client(client)