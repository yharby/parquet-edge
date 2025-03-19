#!/bin/bash

# Activate the Python virtual environment
source "/root/.virtualenvs/pimoroni/bin/activate"

# Log file for tracking sync operations
LOG_FILE="/root/parqtime/parquet-edge/sync_logs.log"

# Get current timestamp
TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")

# Echo start of sync operation to log
echo "[$TIMESTAMP] Starting sync operation" >> $LOG_FILE

# Print environment info
echo "[$TIMESTAMP] Environment: PATH=$PATH" >> $LOG_FILE
echo "[$TIMESTAMP] AWS credentials: $(if [ -n "$AWS_ACCESS_KEY_ID" ]; then echo "Set"; else echo "Not set"; fi)" >> $LOG_FILE
echo "[$TIMESTAMP] AWS endpoint URL: $AWS_ENDPOINT_URL" >> $LOG_FILE

# Run the sync command with verbose output
echo "[$TIMESTAMP] Running sync command..." >> $LOG_FILE
aws s3 sync /root/parqtime/parquet-edge/output s3://youssef-harby/weather-station-realtime-parquet/ --endpoint-url=$AWS_ENDPOINT_URL >> $LOG_FILE 2>&1

# Get the exit code
EXIT_CODE=$?

# Log the result
if [ $EXIT_CODE -eq 0 ]; then
    echo "[$TIMESTAMP] Sync completed successfully" >> $LOG_FILE
else
    echo "[$TIMESTAMP] Sync failed with exit code $EXIT_CODE" >> $LOG_FILE
    
    # Add basic error information
    echo "[$TIMESTAMP] AWS CLI version: $(aws --version 2>&1)" >> $LOG_FILE
fi

# Deactivate the virtual environment
deactivate

exit $EXIT_CODE
