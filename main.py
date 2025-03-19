import time
import datetime
import pandas as pd
from pathlib import Path

# Import sensor libraries
from enviroplus import gas
from bme280 import BME280

# Optional: Light sensor (LTR559) and particulate sensor (PMS5003)
try:
    from ltr559 import LTR559
    ltr559_sensor = LTR559()
except ImportError:
    ltr559_sensor = None

try:
    from pms5003 import PMS5003, ReadTimeoutError
    pms5003 = PMS5003()
except Exception:
    pms5003 = None

# --- Configuration ---
SENSOR_CONFIG = {
    'bme280': True,   # temperature, pressure, humidity
    'gas': True,      # oxidised, reducing, nh3
    'ltr559': True,   # lux, proximity
    'pms5003': False  # pm1, pm2.5, pm10 (set to True if sensor is connected)
}

READ_INTERVAL = 1        # seconds between sensor reads
BATCH_DURATION = 60      # seconds for each batch (will be adjusted to align with minute boundaries)
STATION_ID = "01"        # station identifier for data partitioning

# Station location (Al Rehab City, New Cairo, Egypt)
STATION_LATITUDE = 30.0626   # degrees North
STATION_LONGITUDE = 31.4916  # degrees East

# --- Sensor Initialization ---
if SENSOR_CONFIG.get('bme280'):
    from smbus2 import SMBus
    bus = SMBus(1)
    bme280 = BME280(i2c_dev=bus)

if SENSOR_CONFIG.get('gas'):
    gas.enable_adc()       # Enable ADC for gas sensor
    gas.set_adc_gain(4.096)  # Set ADC gain as appropriate

# --- Sensor Reading Function ---
def read_sensors():
    """
    Reads data from the enabled sensors and returns a dict with a timestamp.
    """
    data = {}
    # Current UTC time
    current_time = datetime.datetime.now(datetime.timezone.utc)
    
    # Use UTC ISO format for the timestamp string
    data['timestamp'] = current_time.isoformat()
    # Add static location data
    data['latitude'] = STATION_LATITUDE
    data['longitude'] = STATION_LONGITUDE
    
    # Add a proper datetime column (commented out)
    # data['datetime'] = current_time
    
    if SENSOR_CONFIG.get('bme280'):
        data['temperature'] = bme280.get_temperature()
        data['pressure'] = bme280.get_pressure()
        data['humidity'] = bme280.get_humidity()
    
    if SENSOR_CONFIG.get('gas'):
        gas_data = gas.read_all()
        # Convert raw sensor values (e.g. in ohms) to kilo-ohms
        data['oxidised'] = gas_data.oxidising / 1000.0
        data['reducing'] = gas_data.reducing / 1000.0
        data['nh3'] = gas_data.nh3 / 1000.0
    
    if SENSOR_CONFIG.get('ltr559') and ltr559_sensor:
        data['lux'] = ltr559_sensor.get_lux()
        data['proximity'] = ltr559_sensor.get_proximity()
    
    if SENSOR_CONFIG.get('pms5003') and pms5003:
        try:
            pm_readings = pms5003.read()
            data['pm1'] = pm_readings.pm_ug_per_m3(1.0)
            data['pm2_5'] = pm_readings.pm_ug_per_m3(2.5)
            data['pm10'] = pm_readings.pm_ug_per_m3(10)
        except ReadTimeoutError:
            data['pm1'] = None
            data['pm2_5'] = None
            data['pm10'] = None

    return data

# --- Main Loop ---
def main():
    print("Starting sensor data collection using fastparquet. Press Ctrl+C to stop.")
    batch_data = []       # List to accumulate sensor readings for the current batch
    
    # Calculate time to the next minute boundary
    now = datetime.datetime.now(datetime.timezone.utc)
    seconds_to_next_minute = 60 - now.second
    if seconds_to_next_minute == 60:  # If we're exactly at a minute boundary
        seconds_to_next_minute = 0
    
    # Adjust batch duration to align with minute boundary for the first batch
    first_batch_duration = seconds_to_next_minute if seconds_to_next_minute > 0 else BATCH_DURATION
    
    print(f"First batch will run for {first_batch_duration} seconds to align with minute boundary")
    batch_start = time.time()
    next_batch_end = batch_start + first_batch_duration

    try:
        while True:
            sensor_data = read_sensors()
            batch_data.append(sensor_data)
            
            current_time = time.time()
            time_to_next_batch = max(0, next_batch_end - current_time)
            
            # Check if it's time to write out the batch
            if current_time >= next_batch_end:
                # Get the current time for the filename
                current_dt = datetime.datetime.now(datetime.timezone.utc)
                # Round down to the nearest minute for the timestamp
                rounded_dt = current_dt.replace(second=0, microsecond=0)
                timestamp = rounded_dt.strftime('%Y%m%dT%H%M%SZ')
                
                # Convert the list of dicts to a Pandas DataFrame
                df = pd.DataFrame(batch_data)
                
                # Ensure the datetime column is properly typed (commented out)
                # if 'datetime' in df.columns:
                #     df['datetime'] = pd.to_datetime(df['datetime'])
                
                # Extract date components for partitioning
                year = rounded_dt.strftime('%Y')
                month = rounded_dt.strftime('%m')
                day = rounded_dt.strftime('%d')
                
                # Generate a filename based on the timestamp
                if BATCH_DURATION <= 60:  # If batch duration is hourly or less
                    filename = f"data_{rounded_dt.strftime('%H%M')}.parquet"
                elif BATCH_DURATION <= 3600:  # If batch duration is daily or less
                    filename = f"data_{rounded_dt.strftime('%H')}.parquet"
                else:
                    filename = f"data.parquet"
                
                # Create partitioned output directory
                output_dir = Path(f"output/station={STATION_ID}/year={year}/month={month}/day={day}")
                output_dir.mkdir(parents=True, exist_ok=True)
                
                # Full path for the output file
                output_path = output_dir / filename
                
                # Write the DataFrame to a Parquet file using fastparquet
                df.to_parquet(output_path, engine='fastparquet', compression='snappy', index=False)
                print(f"Wrote {len(batch_data)} records to {output_path}")
                
                # Reset the batch data for the next cycle
                batch_data = []
                
                # Set the next batch end time to align with minute boundaries
                next_batch_end = next_batch_end + BATCH_DURATION
                # Ensure we're aligned to minute boundaries
                current_dt = datetime.datetime.fromtimestamp(next_batch_end, datetime.timezone.utc)
                if current_dt.second != 0:
                    # Adjust to the next minute boundary
                    seconds_to_add = 60 - current_dt.second
                    next_batch_end += seconds_to_add
            
            # Sleep until next reading or end of batch, whichever comes first
            sleep_time = min(READ_INTERVAL, time_to_next_batch)
            if sleep_time > 0:
                time.sleep(sleep_time)
                
    except KeyboardInterrupt:
        print("Stopping sensor data collection.")

if __name__ == '__main__':
    main()
