# Import libraries
import psycopg2
from psycopg2 import sql, Error as pg_err, OperationalError as pg_op_err, InterfaceError as pg_int_err
import json


# Template for inserting new error rows
# into the error table.
error_template = 'INSERT INTO "errorTable" (app_id, error, device) VALUES(%s, %s, %s) ON CONFLICT ON CONSTRAINT unique_error_src DO UPDATE SET timestamp=NOW()'


# Logs an error to the error table and
# correlates it with the responsible user.
def log_error(errlog_connection, app_id, device, error_str):
    try:
        with errlog_connection.cursor() as cursor:
            try:
                cursor.execute(error_template, (app_id, error_str, device))
                errlog_connection.commit()
            except Exception as err:
                errlog_connection.rollback()
                print('[!!!] error when trying to log error')
                print(err)
    except Exception as err:
        print('[ !!! ] error logger app_id logic failure')
        print(err)


# Inserts an Intermediate Representation (IR)
# formatted message into the database.
def insert_ir_message(db_connection, errlog_connection, app_id, ir_message):
    # Incoming message is a parsed IR-formed json

    # There must be payload in the incoming message
    payload_length = len(ir_message['payload'])
    try:
        assert(payload_length > 0)
    except AssertionError:
        print('[  !  ] empty payload error')
        try:
            device = ir_message['device']
        except KeyError:
            device = ''
        log_error(errlog_connection, app_id, device, 'assertion failure: received empty payload (no metrics)')
        return

    # Extract top-level elements
    original_app_id = app_id
    app_id = sql.SQL('{}').format(sql.Identifier(app_id)).as_string(db_connection)
    unix_timestamp = ir_message['time']

    # Add metadata column names to the insert statement
    metadata_values = [value for value in ir_message['metadata'].values()]
    metadata_keys = list(ir_message['metadata'].keys())
    
    # If any string data is provided, it needs to be
    # appended to the metadata key list in order to
    # be a part of the subsequent query templates.
    str_data_index_lookup = {}
    for metric, value in ir_message['payload'].items():
        # NOTE: This is the same loop that happens in a
        #       subsequent control block. This is because
        #       the presence of a metadata key at any
        #       position will affect the default query
        #       structure for all other keys.
        if type(value) == str:
            try:
                key_index = metadata_keys.index(metric)
                str_data_index_lookup[metric] = key_index
            except ValueError:
                metadata_keys.append(metric)
                metadata_values.append(None)
                str_data_index_lookup[metric] = len(metadata_keys) - 1
    
    # Aggregate columns and values
    # md_specifiers = ', %s'*len(ir_message['metadata'])
    # md_specifiers = (', ' if len(ir_message['metadata']) > 0 else '') + ', '.join([f'{{{md_field}}}' for md_field in ir_message['metadata']])
    md_specifiers = ', {}'*len(metadata_keys)

    columns_as_identifiers = [sql.Identifier(column) for column in metadata_keys]
    column_template = sql.SQL('(time, metric, value' + md_specifiers + ')') \
                         .format(*columns_as_identifiers) \
                         .as_string(db_connection)

    md_value_specifiers = ', %s'*len(metadata_keys)
    # Create value templates. Each row has at least time,
    # metric, and value columns. Additionally, there may
    # be a variable number of metadata columns.
    value_template = '(to_timestamp(%s), %s, %s' + md_value_specifiers + ')'
    
    # Stores parameters to fill each %s in the eventual insertion string
    parameters = []

    # Add payload data to the insert statement.
    for metric, value in ir_message['payload'].items():
        if type(value) == str:
            # This is a string datapoint. We need to
            # supply a dummy value.
            ins_value = 0

            # We also have to replace the corresponding
            # "metadata" column's placeholder with the
            # provided data.
            ins_metadata_values = metadata_values.copy()
            ins_metadata_values[str_data_index_lookup[metric]] = value
        else:
            ins_value = value
            ins_metadata_values = metadata_values
        
        point_values = [
            unix_timestamp,
            metric,
            ins_value
        ]

        # Add parameters to the query template.
        point_values.extend(ins_metadata_values)
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
    # print(query_template)
    # print(parameters)

    with db_connection.cursor() as cursor:
        try:
            cursor.execute(query_template, tuple(parameters))
            db_connection.commit()
        except pg_int_err as err:
            print(f'[  !  ] pgsql InterfaceError!\n{str(err)}')
        except pg_op_err as err:
            print(f'[  !  ] pgsql OperationalError!\n{str(err)}')
        except pg_err as err:
            # An explicit rollback is not strictly necessary here.
            # Per the docs: "Closing a connection without committing 
            #                the changes first will cause an implicit 
            #                rollback to be performed."
            db_connection.rollback()
            try:
                device = ir_message['device']
            except KeyError:
                device = ''

            if str(err) == 'can\'t adapt type \'dict\'':
                # An object was provided instead of a primitive type
                log_error(errlog_connection, original_app_id, device, 'unexpected object (expected primitive)')
            elif err.pgcode == '42P01':
                # App not found
                log_error(errlog_connection, original_app_id, device, 'invalid app')
            elif err.pgcode == '42703':
                # Invalid metadata key provided
                log_error(errlog_connection, original_app_id, device, 'invalid metadata')
            elif err.pgcode == '22P02':
                # Provided a string for a column that should be a double precision
                log_error(errlog_connection, original_app_id, device, 'unexpected string (expected number)')
            elif err.pgcode == '42804':
                # Provided a boolean for a column that should be a double precision
                log_error(errlog_connection, original_app_id, device, 'unexpected boolean (expected number)')
            else:
                print('[ !!! ] unhandled sql error,', err)
                print(err.pgcode)
        except Exception as err:
            print('[!!!!!] unexpected non-sql error on insertion', err)
            db_connection.rollback()


# Attempts to create a db connection object
# and returns the connection or False if
# unsuccessful.
def create_connection(ts_user, ts_passwd, ts_host, ts_port, ts_database):
    try:
        # Template format string for the connection URL
        connection_url = f'postgres://{ts_user}:{ts_passwd}@{ts_host}:{ts_port}/{ts_database}?sslmode=require'

        # Connect to the db and store the connection object
        connection = psycopg2.connect(connection_url, sslmode='require')
        return connection
    except Exception as err:
        print('[!!!!!] failed to establish database connection', err)
        return False
