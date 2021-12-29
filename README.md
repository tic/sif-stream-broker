# SIF-stream-broker

### Purpose
* Receive pre-processed messages from the [ingest-broker](https://github.com/sif-ingest-broker)
* Transform the data blobs from IR form into TimescaleDB insertion queries
* Escape any SQL injection attempts
* Execute the insertion queries generated from the data blob

## How to run
In the SIF ecosystem, the main file in this repo -- `sif_stream_broker.py` -- is running in an AWS EC2 instance. This instance hosts an MQTT broker that receives messages from the ingest-broker processing running elsewhere. There are several prerequisites in order to run this script *as-is*. For each requirement, listed below, the version used by our EC2 instance is included, if applicable:

1. Python v3.7.10
2. pip v20.2.2 (python 3.7)
3. mosquitto v3.1.1
4. A proper `.env` file, details below
5. A CA certificate for your PostgreSQL database (i.e. TimescaleDB)

###Environment File
The `.env` file should be placed in the root directory of the project, i.e. at the same level as `app.js`. The file should define the following set of values:

Field | Value
----- | -----
ENV | enum ["DEVELOPMENT", "PRODUCTION"]
INGEST_STREAM | The address where the script can access the MQTT broker that is receiving pre-processed, IR formatted, data. **Note: unlike the ingest-broker, do not include "mqtt://" in this variable.**
TS_USER | PostgreSQL database username
TS_PASSWD | PostgreSQL database password
TS_HOST | PostgreSQL database hostname
TS_PORT | PostgreSQL database port
TS_DATABASE | PostgreSQL database name
PGSSLROOTCERT | `/path/to/ca.pem` (path to CA certificate, used for opening an SSL connection with the database)

### Launching the script
Run the command:

    python3 sif_stream_broker.py

#### Our launch script
In our implementation of the SIF platform, we have a bash script that runs when our EC2 instances launch. While the single command above is sufficient for testing, this script may be useful in scenarios that necessitate a higher degree of automation. It uses `tmux`, a terminal multiplexing utility similar to the native `screen` utility.

```bash
#!/bin/bash

# Spawn the MQTT broker (SIF-STREAM-BROKER)
tmux new -s mosquitto -d
tmux send-keys -t "mosquitto" "mosquitto" Enter

# Spawn the data stream handler
tmux new -s stream-broker -d
tmux send-keys -t "stream-broker" "cd ~/sif-stream-broker" Enter
tmux send-keys -t "stream-broker" "python3 sif_stream_broker.py" Enter
```

## Sending data to the stream-broker
Unlike the ingest-broker, the stream-broker should never receive data from more than one place. While the ingest-broker may receive data from an arbitrary number of sources (many users, devices, etc.), the stream-broker only receives messages from the ingest-broker. In our real-world setup of the SIF platform, the EC2 instance running the stream broker blocks traffic from all sources *but* the ingest-broker. If you have spooled up this script for testing, make sure your tests abide by this principle if you want to stay true to the intended use case of this script.
