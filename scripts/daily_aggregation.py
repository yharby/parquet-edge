#!/usr/bin/env python
"""
Daily Aggregation Script

This script performs daily aggregation of weather station data using DuckDB to:
1. Read Parquet files from S3
2. Aggregate data (simple copy in this case)
3. Write results back to S3
"""

import os
import sys
from datetime import datetime, timedelta
import duckdb
from dotenv import load_dotenv

# Load environment variables from .env file if it exists
load_dotenv()

def get_yesterday_date():
    """Get yesterday's date components."""
    yesterday = datetime.now() - timedelta(days=1)
    return {
        'year': yesterday.strftime('%Y'),
        'month': yesterday.strftime('%m'),
        'day': yesterday.strftime('%d')
    }

def main():
    # Get yesterday's date components (or use from environment if set)
    date_parts = get_yesterday_date()
    year = os.environ.get('YESTERDAY_YEAR', date_parts['year'])
    month = os.environ.get('YESTERDAY_MONTH', date_parts['month'])
    day = os.environ.get('YESTERDAY_DAY', date_parts['day'])
    
    print(f"Aggregating data for: {year}-{month}-{day}")
    
    # Construct S3 paths
    input_path = f"s3://youssef-harby/weather-station-realtime-parquet/parquet/station=01/year={year}/month={month}/day={day}/*.parquet"
    output_file = f"{year}_{month}_{day}.parquet"
    dest_path = f"s3://youssef-harby/weather-station-realtime-parquet/1m_avg_daily/station=01/year={year}/month={month}/{output_file}"
    
    print(f"Input path: {input_path}")
    print(f"Output path: {dest_path}")
    
    # Initialize DuckDB connection
    conn = duckdb.connect(database=':memory:')
    
    try:
        # Install and load httpfs extension
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        
        # Configure S3 settings
        conn.execute("SET s3_uploader_max_parts_per_file = 1000000;")
        conn.execute("SET s3_uploader_max_filesize = '5GB';")
        conn.execute("SET s3_uploader_thread_limit = 10;")
        conn.execute("SET s3_region='us-west-2';")
        
        # Create S3 secret using credential_chain provider
        conn.execute("""
        CREATE OR REPLACE SECRET s3_secret (
            TYPE S3,
            PROVIDER credential_chain,
            CHAIN 'env',
            ENDPOINT 'data.source.coop',
            URL_STYLE 'path'
        );
        """)
        
        # Count source rows to verify data exists
        print("Counting source rows...")
        count_result = conn.execute(f"SELECT COUNT(*) AS source_row_count FROM read_parquet('{input_path}')").fetchone()
        source_row_count = count_result[0]
        print(f"Source row count: {source_row_count}")
        
        if source_row_count == 0:
            print("No data found for the specified date.")
            return 1
        
        # Perform aggregation and direct upload to S3
        print("Performing aggregation and upload...")
        conn.execute(f"""
        COPY (
          SELECT
            date_trunc('minute', timestamp) AS timestamp,
            AVG(temperature) AS temperature,
            AVG(raw_temperature) AS raw_temperature,
            AVG(pressure) AS pressure,
            AVG(humidity) AS humidity,
            AVG(oxidised) AS oxidised,
            AVG(reducing) AS reducing,
            AVG(nh3) AS nh3,
            AVG(lux) AS lux,
            AVG(proximity) AS proximity,
            AVG(pm1) AS pm1,
            AVG(pm2_5) AS pm2_5,
            AVG(pm10) AS pm10,
            AVG(particles_03um) AS particles_03um,
            AVG(particles_05um) AS particles_05um,
            AVG(particles_10um) AS particles_10um,
            AVG(particles_25um) AS particles_25um,
            AVG(particles_50um) AS particles_50um,
            AVG(particles_100um) AS particles_100um
          FROM 
            read_parquet('{input_path}', hive_partitioning=1, union_by_name=true)
          GROUP BY 
            date_trunc('minute', timestamp)
          ORDER BY 
            timestamp
        ) TO '{dest_path}' (FORMAT 'parquet', COMPRESSION 'zstd', ROW_GROUP_SIZE 65536, OVERWRITE_OR_IGNORE);
        """)
        
        # Count rows in the destination file to verify successful upload
        print("Verifying destination file...")
        dest_count_result = conn.execute(f"SELECT COUNT(*) AS destination_row_count FROM read_parquet('{dest_path}')").fetchone()
        destination_row_count = dest_count_result[0]
        print(f"Destination row count: {destination_row_count}")
        
        # Verify counts match
        if source_row_count != destination_row_count:
            print(f"Warning: Source count ({source_row_count}) does not match destination count ({destination_row_count})")
        
        print("Daily aggregation completed successfully.")
        return 0
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1
    finally:
        conn.close()

if __name__ == "__main__":
    sys.exit(main())
