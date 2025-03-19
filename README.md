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
