# Import libraries
import psycopg2
from psycopg2 import sql
import json


# Inserts an Intermediate Representation (IR)
# formatted message into the database.
def insert_ir_message(db_connection, app_id, ir_message):
    # Incoming message is a parsed IR-formed json

    # There must be payload in the incoming message
    payload_length = len(ir_message['payload'])
    assert(payload_length > 0)

    # Extract top-level elements
    app_id = sql.SQL('{}').format(sql.Identifier(app_id)).as_string(db_connection)
    unix_timestamp = ir_message['time']

    # Aggregate columns and values
    # md_specifiers = ', %s'*len(ir_message['metadata'])
    # md_specifiers = (', ' if len(ir_message['metadata']) > 0 else '') + ', '.join([f'{{{md_field}}}' for md_field in ir_message['metadata']])
    md_specifiers = ', {}'*len(ir_message['metadata'])

    columns_as_identifiers = [sql.Identifier(column) for column in ir_message['metadata'].keys()]
    column_template = sql.SQL('(time, metric, value' + md_specifiers + ')') \
                         .format(*columns_as_identifiers) \
                         .as_string(db_connection)

    md_value_specifiers = ', %s'*len(ir_message['metadata'])
    # Create value templates. Each row has at least time,
    # metric, and value columns. Additionally, there may
    # be a variable number of metadata columns.
    value_template = '(to_timestamp(%s), %s, %s' + md_value_specifiers + ')'

    
    # Stores parameters to fill each %s in the eventual insertion string
    parameters = []


    # Add metadata column names to the insert statement
    metadata_values = [value for value in ir_message['metadata'].values()]


    # Add payload data to the insert statement
    for metric, value in ir_message['payload'].items():
        point_values = [
            unix_timestamp,
            metric,
            value
        ]
        point_values.extend(metadata_values)
        parameters.extend(point_values)


    # Construct template insert string
    # NOTE: While format strings are generally vulnerable to SQL
    #       injection attacks, its usage here is believed to be 
    #       SAFE for two reasons:
    #         a) it originates from the ingest-broker, which has
    #            logic to safe all app ids that it sees;
    #         b) the app_id variable is fed through a SQL identifier
    #            prior to the usage of a format string here
    query_template = f'INSERT INTO {app_id} ' \
        + column_template \
        + ' VALUES ' \
        + value_template \
        + (', ' + value_template)*(payload_length - 1) \
        + ';'
    print(query_template)
    print(parameters)

    with db_connection.cursor() as cursor:
        try:
            cursor.execute(query_template, tuple(parameters))
            db_connection.commit()
        except Exception:
            # An explicit rollback is not strictly necessary here.
            # Per the docs: "Closing a connection without committing 
            #                the changes first will cause an implicit 
            #                rollback to be performed."
            db_connection.rollback()


# Attempts to create a db connection object
# and returns the connection or False if
# unsuccessful.
def create_connection(ts_user, ts_passwd, ts_host, ts_port):
    try:
        # Template format string for the connection URL
        connection_url = f'postgres://{ts_user}:{ts_passwd}@{ts_host}:{ts_port}/tsdb?sslmode=require'

        # Connect to the db and store the connection object
        connection = psycopg2.connect(connection_url)
        return connection
    except Exception as err:
        print(err)
        return False
