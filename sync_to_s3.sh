#!/bin/bash

# Activate the Python virtual environment
source "/root/.virtualenvs/pimoroni/bin/activate"

# Log file for tracking sync operations
LOG_FILE="/root/parqtime/parquet-edge/sync_logs.log"

# Get current timestamp
TIMESTAMP=$(date "+%Y-%m-%d %H:%M:%S")

# Echo start of sync operation to log
echo "[$TIMESTAMP] Starting sync operation" >> $LOG_FILE

# Print AWS CLI version for debugging
echo "[$TIMESTAMP] AWS CLI version:" >> $LOG_FILE
aws --version >> $LOG_FILE 2>&1

# Print environment info
echo "[$TIMESTAMP] Environment: PATH=$PATH" >> $LOG_FILE
echo "[$TIMESTAMP] Current directory: $(pwd)" >> $LOG_FILE
echo "[$TIMESTAMP] Output directory exists: $(if [ -d "/root/parqtime/parquet-edge/output" ]; then echo "Yes"; else echo "No"; fi)" >> $LOG_FILE

# Run the sync command with verbose output
echo "[$TIMESTAMP] Running sync command..." >> $LOG_FILE
aws s3 sync /root/parqtime/parquet-edge/output s3://youssef-harby/weather-station-realtime-parquet/ --endpoint-url=https://data.source.coop --debug >> $LOG_FILE 2>&1

# Get the exit code
EXIT_CODE=$?

# Log the result
if [ $EXIT_CODE -eq 0 ]; then
    echo "[$TIMESTAMP] Sync completed successfully" >> $LOG_FILE
else
    echo "[$TIMESTAMP] Sync failed with exit code $EXIT_CODE" >> $LOG_FILE
    
    # Add more detailed error information
    echo "[$TIMESTAMP] Checking AWS credentials:" >> $LOG_FILE
    aws configure list >> $LOG_FILE 2>&1
    
    echo "[$TIMESTAMP] Testing S3 connectivity:" >> $LOG_FILE
    aws s3 ls --endpoint-url=https://data.source.coop >> $LOG_FILE 2>&1
fi

# Deactivate the virtual environment
deactivate

exit $EXIT_CODE
