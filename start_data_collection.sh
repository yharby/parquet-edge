#!/bin/bash

# Script to start the data collection with retry logic
LOG_FILE="/root/parqtime/parquet-edge/main_output.log"
MAX_RETRIES=3
RETRY_DELAY=60  # seconds

# Function to log messages
log_message() {
    echo "$(date '+%Y-%m-%d %H:%M:%S'): $1" >> "$LOG_FILE"
}

# Activate virtual environment
activate_venv() {
    source "/root/.virtualenvs/pimoroni/bin/activate"
}

# Change to the project directory
cd /root/parqtime/parquet-edge || {
    log_message "Failed to change to project directory"
    exit 1
}

# Initial delay to ensure hardware is ready
log_message "Waiting 60 seconds for hardware initialization..."
sleep 60

# Try to run the script with retries
retry_count=0
while [ $retry_count -lt $MAX_RETRIES ]; do
    log_message "Starting data collection (attempt $((retry_count+1))/$MAX_RETRIES)"
    
    # Activate the virtual environment and run the script
    activate_venv
    python main.py >> "$LOG_FILE" 2>&1
    
    # Check if the script exited with an error
    if [ $? -ne 0 ]; then
        retry_count=$((retry_count+1))
        if [ $retry_count -lt $MAX_RETRIES ]; then
            log_message "Data collection failed. Retrying in $RETRY_DELAY seconds..."
            sleep $RETRY_DELAY
        else
            log_message "Maximum retry attempts reached. Giving up."
            exit 1
        fi
    else
        # Script exited successfully
        log_message "Data collection completed successfully"
        exit 0
    fi
done

exit 1
