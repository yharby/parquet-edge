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

# Handle PMS5003 sensor - gracefully handle if it's broken
try:
    from pms5003 import PMS5003, ReadTimeoutError
    pms5003 = PMS5003()
    print("PMS5003 sensor initialized successfully")
except Exception as e:
    print(f"Warning: PMS5003 sensor initialization failed: {e}")
    print("Continuing without particulate matter sensor")
    pms5003 = None

# --- Configuration ---
SENSOR_CONFIG = {
    'bme280': True,   # temperature, pressure, humidity
    'gas': True,      # oxidised, reducing, nh3
    'ltr559': True,   # lux, proximity
    'pms5003': True   # pm1, pm2.5, pm10 (set to True if sensor is connected)
}

READ_INTERVAL = 1        # seconds between sensor reads
BATCH_DURATION = 300      # seconds for each batch (will be adjusted to align with minute boundaries)
STATION_ID = "01"        # station identifier for data partitioning

# Station location (Al Rehab City, New Cairo, Egypt)
STATION_LATITUDE = 30.0626   # degrees North
STATION_LONGITUDE = 31.4916  # degrees East

# Temperature compensation settings
TEMP_COMPENSATION_ENABLED = True  # Set to False to use raw temperature readings
TEMP_COMPENSATION_FACTOR = 2.25   # Tuning factor - adjust as needed
cpu_temps = []  # List to store CPU temperature history for smoothing

# --- Sensor Initialization ---
if SENSOR_CONFIG.get('bme280'):
    from smbus2 import SMBus
    bus = SMBus(1)
    bme280 = BME280(i2c_dev=bus)

if SENSOR_CONFIG.get('gas'):
    gas.enable_adc()       # Enable ADC for gas sensor
    gas.set_adc_gain(4.096)  # Set ADC gain as appropriate

# --- Helper Functions ---
def get_cpu_temperature():
    """
    Get the CPU temperature for compensating BME280 temperature readings.
    """
    try:
        with open("/sys/class/thermal/thermal_zone0/temp", "r") as f:
            temp = f.read()
            temp = int(temp) / 1000.0
        return temp
    except (IOError, ValueError):
        # Return a safe default if we can't read the CPU temperature
        return 40.0

def compensate_temperature(raw_temp):
    """
    Compensate temperature reading from BME280 using CPU temperature.
    Method adapted from Initial State's Enviro pHAT review.
    """
    global cpu_temps
    
    if not TEMP_COMPENSATION_ENABLED:
        return raw_temp
        
    # Get current CPU temperature
    cpu_temp = get_cpu_temperature()
    
    # Initialize the list if it's empty
    if len(cpu_temps) == 0:
        cpu_temps = [cpu_temp] * 5
    else:
        # Smooth out with some averaging to decrease jitter
        cpu_temps = cpu_temps[1:] + [cpu_temp]
    
    # Calculate the average CPU temperature
    avg_cpu_temp = sum(cpu_temps) / float(len(cpu_temps))
    
    # Apply compensation formula
    comp_temp = raw_temp - ((avg_cpu_temp - raw_temp) / TEMP_COMPENSATION_FACTOR)
    
    return comp_temp

# --- Sensor Reading Function ---
def read_sensors():
    """
    Reads data from the enabled sensors and returns a dict with a timestamp.
    """
    data = {}
    # Current UTC time
    current_time = datetime.datetime.now(datetime.timezone.utc)
    
    # Store timestamp as a native datetime object instead of a string
    data['timestamp'] = current_time
    # Add static location data
    data['latitude'] = STATION_LATITUDE
    data['longitude'] = STATION_LONGITUDE
    
    # Add a proper datetime column (commented out)
    # data['datetime'] = current_time
    
    if SENSOR_CONFIG.get('bme280'):
        raw_temp = bme280.get_temperature()
        data['temperature'] = compensate_temperature(raw_temp)
        data['raw_temperature'] = raw_temp  # Store the raw value as well
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
            # Standard PM measurements
            data['pm1'] = float(pm_readings.pm_ug_per_m3(1.0))
            data['pm2_5'] = float(pm_readings.pm_ug_per_m3(2.5))
            data['pm10'] = float(pm_readings.pm_ug_per_m3(10.0))
            # Particle counts - using the correct attribute names
            data['particles_03um'] = float(pm_readings.pm_per_1l_air(0.3))
            data['particles_05um'] = float(pm_readings.pm_per_1l_air(0.5))
            data['particles_10um'] = float(pm_readings.pm_per_1l_air(1.0))
            data['particles_25um'] = float(pm_readings.pm_per_1l_air(2.5))
            data['particles_50um'] = float(pm_readings.pm_per_1l_air(5.0))
            data['particles_100um'] = float(pm_readings.pm_per_1l_air(10.0))
        except (ReadTimeoutError, ValueError) as e:
            print(f"Warning: PMS5003 read error: {e}")
            # Set all PM-related fields to None in case of timeout or value error
            for field in ['pm1', 'pm2_5', 'pm10',
                         'particles_03um', 'particles_05um', 'particles_10um',
                         'particles_25um', 'particles_50um', 'particles_100um']:
                data[field] = None

    return data

# --- Main Loop ---
def main():
    print("Starting sensor data collection using fastparquet. Press Ctrl+C to stop.")
    batch_data = []       # List to accumulate sensor readings for the current batch
    
    # Calculate time to the next 5-minute boundary
    now = datetime.datetime.now(datetime.timezone.utc)
    # Calculate seconds to next 5-minute boundary
    minutes_to_add = 5 - (now.minute % 5)
    if minutes_to_add == 5 and now.second == 0:  # If we're exactly at a 5-minute boundary
        seconds_to_next_5min = 0
    else:
        seconds_to_next_5min = (minutes_to_add * 60) - now.second
    
    # Adjust batch duration to align with 5-minute boundary for the first batch
    first_batch_duration = seconds_to_next_5min if seconds_to_next_5min > 0 else BATCH_DURATION
    
    print(f"First batch will run for {first_batch_duration} seconds to align with 5-minute boundary")
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
                # Get the batch end time for the filename (this is when the data collection period ended)
                batch_end_dt = datetime.datetime.fromtimestamp(next_batch_end, datetime.timezone.utc)
                # Round to the nearest 5-minute boundary for the timestamp
                rounded_minute = 5 * (batch_end_dt.minute // 5)
                rounded_dt = batch_end_dt.replace(minute=rounded_minute, second=0, microsecond=0)
                timestamp = rounded_dt.strftime('%Y%m%dT%H%M%SZ')
                
                # Convert the list of dicts to a Pandas DataFrame
                df = pd.DataFrame(batch_data)
                
                # Ensure the timestamp column is properly typed as datetime and reduce precision to seconds
                if 'timestamp' in df.columns:
                    # First convert to datetime
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    # Then reduce precision to seconds (removes microseconds/nanoseconds)
                    df['timestamp'] = df['timestamp'].dt.floor('s')
                
                # Extract date components for partitioning
                year = rounded_dt.strftime('%Y')
                month = rounded_dt.strftime('%m')
                day = rounded_dt.strftime('%d')
                
                # Generate a filename based on the batch end time
                if BATCH_DURATION <= 300:  # If batch duration is 5 minutes or less
                    filename = f"data_{rounded_dt.strftime('%H%M')}.parquet"
                elif BATCH_DURATION <= 3600:  # If batch duration is hourly or less
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
                
                # Set the next batch end time to align with 5-minute boundaries
                next_batch_end = next_batch_end + BATCH_DURATION
                # Ensure we're aligned to 5-minute boundaries
                current_dt = datetime.datetime.fromtimestamp(next_batch_end, datetime.timezone.utc)
                # Calculate seconds to the next 5-minute boundary
                minutes_to_add = 5 - (current_dt.minute % 5)
                if minutes_to_add == 5 and current_dt.second == 0:  # If we're exactly at a 5-minute boundary
                    pass  # No adjustment needed
                else:
                    # Adjust to the next 5-minute boundary
                    seconds_to_add = (minutes_to_add * 60) - current_dt.second
                    next_batch_end += seconds_to_add
            
            # Sleep until next reading or end of batch, whichever comes first
            sleep_time = min(READ_INTERVAL, time_to_next_batch)
            if sleep_time > 0:
                time.sleep(sleep_time)
                
    except KeyboardInterrupt:
        print("Stopping sensor data collection.")

if __name__ == '__main__':
    main()
