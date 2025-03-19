#!/bin/bash

# Log file for tracking sync operations
LOG_FILE="/root/parqtime/parquet-edge/sync_logs.log"

# Get current timestamp
TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")

# Echo start of sync operation to log
echo "[$TIMESTAMP] Starting sync operation" >> $LOG_FILE

# Run the sync command
aws s3 sync /root/parqtime/parquet-edge/output s3://youssef-harby/weather-station-realtime-parquet/ --endpoint-url=https://data.source.coop

# Get the exit code
EXIT_CODE=$?

# Log the result
if [ $EXIT_CODE -eq 0 ]; then
    echo "[$TIMESTAMP] Sync completed successfully" >> $LOG_FILE
else
    echo "[$TIMESTAMP] Sync failed with exit code $EXIT_CODE" >> $LOG_FILE
fi

exit $EXIT_CODE
