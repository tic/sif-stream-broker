# Import necessary libraries
import paho.mqtt.client as mqtt
from datetime import datetime
import json


# Import database connector and create a blank
# connection object
import db
db_connection = None
errlog_connection = None

# Load env variables and the function to read them
from os import getenv as os_getenv
from dotenv import load_dotenv
load_dotenv()


# Executes when a message arrives in the ingest stream
def on_message_receive(client, userdata, message):
    try:
        # In the unexpected event of a parsing or
        # decoding failure, this ensures the error
        # will not result in a faulty log and that
        # is will be reported to a sysadmin's acct.
        parsed = {
            'app_id': 'gmf_STREAM PARSE ERROR'
        }
        message = message.payload.decode("utf-8")
        #print(message)
        parsed = json.loads(message)
        db.insert_ir_message(db_connection, errlog_connection, parsed['app_id'], parsed['data'])
    except Exception as outer_err:
        try:
            try:
                device = parsed['device']
            except KeyError:
                device = ''
            db.log_error(errlog_connection, parsed['app_id'], device, str(outer_err))
        except Exception as inner_err:
            print('[ !!! ] unhandled exception in message handler error logger', inner_err)


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
def setup_client(client, channel):
    # Connect to the ingest stream host
    if os_getenv('ENV') == 'DEVELOPMENT':
        client.connect('localhost', keepalive = 30)
    else:
        client.connect(os_getenv('INGEST_STREAM'), keepalive = 30)

    # Initiate the client's event loop
    client.loop_start()

    # Subscribe to the ingest data stream
    client.subscribe('ingest/stream/' + channel)

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
    # Get channel from argv
    from sys import argv
    if len(argv) != 2:
        raise Exception("Bad command line arguments. Provide a channel argument and nothing else.")
    channel = argv[1]
    print(f'[  !  ] using channel {channel}')

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

    errlog_connection = db.create_connection(
        os_getenv('TS_USER'), 
        os_getenv('TS_PASSWD'), 
        os_getenv('TS_HOST'), 
        os_getenv('TS_PORT'),
        os_getenv('TS_DATABASE_ERRORS')
    )

    if not db_connection or not errlog_connection:
        print('[ !!! ] failed to establish initial database connection')
        raise Exception('Failed to connect to database')

    # Prepare the client to receive messages
    setup_client(client, channel)

    # Loop indefinitely until stopped by a user
    try:
        while True:
            sleep(60)

            # Automatically reconnect to the database if the connection is severed
            if db_connection.closed:
                print('[ !!  ] recreating database connection')
                db_connection = db.create_connection(
                    os_getenv('TS_USER'), 
                    os_getenv('TS_PASSWD'), 
                    os_getenv('TS_HOST'), 
                    os_getenv('TS_PORT'),
                    os_getenv('TS_DATABASE')
                )

            if errlog_connection.closed:
                print('[ !!  ] recreating database connection')
                errlog_connection = db.create_connection(
                    os_getenv('TS_USER'), 
                    os_getenv('TS_PASSWD'), 
                    os_getenv('TS_HOST'), 
                    os_getenv('TS_PORT'),
                    os_getenv('TS_DATABASE_ERRORS')
                )

    except KeyboardInterrupt:
        pass

    # Clean up the client before shutting down
    cleanup_client(client)