import time
import datetime
import pandas as pd

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
BATCH_DURATION = 60      # seconds for each batch

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
    # Use UTC ISO format for the timestamp
    data['timestamp'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    
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
    batch_start = time.time()

    try:
        while True:
            sensor_data = read_sensors()
            batch_data.append(sensor_data)
            time.sleep(READ_INTERVAL)
            
            # Check if it's time to write out the batch
            if time.time() - batch_start >= BATCH_DURATION:
                # Convert the list of dicts to a Pandas DataFrame
                df = pd.DataFrame(batch_data)
                # Generate a filename with a UTC timestamp
                filename = f"sensor_data_{datetime.datetime.now(datetime.timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.parquet"
                # Write the DataFrame to a Parquet file using fastparquet
                df.to_parquet(filename, engine='fastparquet', compression='snappy', index=False)
                print(f"Wrote {len(batch_data)} records to {filename}")
                # Reset the batch data and timer for the next cycle
                batch_data = []
                batch_start = time.time()
    except KeyboardInterrupt:
        print("Stopping sensor data collection.")

if __name__ == '__main__':
    main()
