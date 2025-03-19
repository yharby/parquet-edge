# Parquet Edge

Collect sensor data from Enviro+ HAT and store it in Parquet format.

## Requirements

- Linux OS (Raspberry Pi recommended)
- Python 3.11+

## Quick Setup

1. **Install Rust** (required for fastparquet's cramjam dependency)
   ```bash
   curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
   source "$HOME/.cargo/env"
   ```

2. **Install system dependencies**
   ```bash
   sudo apt update
   sudo apt install -y autoconf automake build-essential cmake libtool patchelf
   sudo apt install -y python3-smbus libportaudio2
   ```

3. **Install uv package manager**
   ```bash
   curl -LsSf https://astral.sh/uv/install.sh | sh
   ```

4. **Set up Python environment**
   ```bash
   uv venv
   source .venv/bin/activate
   uv pip install wheel Cython setuptools
   uv sync
   ```

5. **Run the application**
   ```bash
   python main.py
   ```

## Features

- Collects data from temperature, pressure, humidity, gas, light, and particulate sensors
- Saves data in efficient Parquet format with minute-aligned timestamps
- Creates organized directory structure for output files

## S3 Synchronization

To automatically sync your Parquet files to an S3 bucket, you can set up a cron job using the provided configuration:

1. **Make the sync script executable**
   ```bash
   chmod +x sync_to_s3.sh
   ```

2. **Configure AWS credentials**
   Ensure your AWS credentials are properly configured on your device, typically in `~/.aws/credentials` or via environment variables.

3. **Set up the cron job**
   ```bash
   # View the current crontab
   crontab -l

   # Edit the crontab (this will open an editor)
   crontab -e

   # Add the contents from crontab_config.txt
   # Or import directly from the file
   crontab crontab_config.txt
   ```

The provided crontab configuration will:
- Automatically start the data collection script (main.py) on system boot
- Sync your output directory to S3 every 5 minutes
- Log all operations to log files
- Automatically rotate logs when they exceed 10MB

You may need to adjust paths in both `crontab_config.txt` and `sync_to_s3.sh` to match your specific installation directory.
