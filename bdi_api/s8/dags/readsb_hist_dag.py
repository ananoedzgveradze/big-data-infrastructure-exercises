import gzip
import json
import logging
from datetime import datetime, timedelta
from io import BytesIO
from typing import List

import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Constants
S3_BUCKET = Variable.get('S3_BUCKET_NAME')
BASE_URL = "https://samples.adsbexchange.com/readsb-hist"
MAX_FILES_PER_DAY = 100

def get_valid_dates() -> List[datetime]:
    """Generate list of first day of each month from 2023/11 to 2024/11"""
    start_date = datetime(2023, 11, 1)
    end_date = datetime(2024, 11, 1)
    dates = []

    current_date = start_date
    while current_date <= end_date:
        if current_date.day == 1:  # Only first day of each month
            dates.append(current_date)
        current_date += timedelta(days=1)

    return dates

def download_readsb_data(**context) -> None:
    """Download readsb-hist data for the execution date"""
    execution_date = context['execution_date']
    s3_hook = S3Hook(aws_conn_id='aws_default')

    # Only process if it's the first day of the month
    if execution_date.day != 1:
        logging.info(f"Skipping {execution_date} as it's not the first day of the month")
        return

    try:
        date_str = execution_date.strftime('%Y/%m/%d')
        base_path = f"{BASE_URL}/{date_str}"

        # Get list of files for the day
        response = requests.get(f"{base_path}/files.json")
        response.raise_for_status()
        files = response.json()

        # Limit to MAX_FILES_PER_DAY files
        files = files[:MAX_FILES_PER_DAY]

        downloaded_files = []
        for file in files:
            try:
                file_url = f"{base_path}/{file}"
                response = requests.get(file_url)
                response.raise_for_status()

                # Store raw data in S3
                raw_key = f"raw/readsb/{date_str}/{file}"
                s3_hook.load_bytes(
                    response.content,
                    key=raw_key,
                    bucket_name=S3_BUCKET,
                    replace=True
                )
                downloaded_files.append(raw_key)

            except Exception as e:
                logging.error(f"Error downloading file {file}: {str(e)}")
                continue

        context['task_instance'].xcom_push(key='downloaded_files', value=downloaded_files)
        logging.info(f"Successfully downloaded {len(downloaded_files)} files for {date_str}")

    except Exception as e:
        logging.error(f"Error downloading readsb data: {str(e)}")
        raise

def process_readsb_data(**context) -> None:
    """Process downloaded readsb-hist data and prepare for database"""
    execution_date = context['execution_date']
    s3_hook = S3Hook(aws_conn_id='aws_default')

    try:
        downloaded_files = context['task_instance'].xcom_pull(
            key='downloaded_files',
            task_ids='download_readsb_data'
        )

        if not downloaded_files:
            logging.info("No files to process")
            return

        all_aircraft_data = []
        for raw_key in downloaded_files:
            try:
                obj = s3_hook.get_key(key=raw_key, bucket_name=S3_BUCKET)
                if not obj:
                    continue

                # Process gzipped JSON data
                with gzip.GzipFile(fileobj=BytesIO(obj.get()['Body'].read())) as gz:
                    data = json.loads(gz.read().decode('utf-8'))

                    # Extract aircraft data
                    for aircraft in data.get('aircraft', []):
                        if 'hex' not in aircraft:
                            continue

                        processed_data = {
                            'timestamp': data.get('now', None),
                            'icao': aircraft['hex'],
                            'flight': aircraft.get('flight', None),
                            'lat': aircraft.get('lat', None),
                            'lon': aircraft.get('lon', None),
                            'altitude': aircraft.get('alt_baro', None),
                            'speed': aircraft.get('gs', None),
                            'track': aircraft.get('track', None),
                            'vertical_rate': aircraft.get('baro_rate', None),
                            'squawk': aircraft.get('squawk', None),
                            'emergency': aircraft.get('emergency', None),
                            'category': aircraft.get('category', None),
                            'nav_qnh': aircraft.get('nav_qnh', None),
                            'nav_altitude_mcp': aircraft.get('nav_altitude_mcp', None),
                            'nav_heading': aircraft.get('nav_heading', None),
                            'version': aircraft.get('version', None),
                            'processed_at': datetime.utcnow()
                        }
                        all_aircraft_data.append(processed_data)

            except Exception as e:
                logging.error(f"Error processing file {raw_key}: {str(e)}")
                continue

        if all_aircraft_data:
            # Convert to DataFrame for easier processing
            df = pd.DataFrame(all_aircraft_data)

            # Store prepared data in S3
            date_str = execution_date.strftime('%Y/%m/%d')
            prepared_key = f"prepared/readsb/{date_str}/aircraft_data.json"
            s3_hook.load_string(
                df.to_json(orient='records'),
                key=prepared_key,
                bucket_name=S3_BUCKET,
                replace=True
            )

            context['task_instance'].xcom_push(key='prepared_key', value=prepared_key)
            logging.info(f"Successfully processed {len(all_aircraft_data)} records")

    except Exception as e:
        logging.error(f"Error processing readsb data: {str(e)}")
        raise

def load_to_postgres(**context) -> None:
    """Load processed readsb data into PostgreSQL"""
    s3_hook = S3Hook(aws_conn_id='aws_default')
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    try:
        prepared_key = context['task_instance'].xcom_pull(
            key='prepared_key',
            task_ids='process_readsb_data'
        )

        if not prepared_key:
            logging.info("No data to load")
            return

        obj = s3_hook.get_key(key=prepared_key, bucket_name=S3_BUCKET)
        if not obj:
            raise Exception(f"Prepared data file not found: {prepared_key}")

        data = json.loads(obj.get()['Body'].read().decode('utf-8'))
        df = pd.DataFrame(data)

        # Create table if not exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS readsb_aircraft_data (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP,
            icao VARCHAR(255),
            flight VARCHAR(255),
            lat FLOAT,
            lon FLOAT,
            altitude INTEGER,
            speed FLOAT,
            track FLOAT,
            vertical_rate INTEGER,
            squawk VARCHAR(10),
            emergency VARCHAR(50),
            category VARCHAR(50),
            nav_qnh FLOAT,
            nav_altitude_mcp INTEGER,
            nav_heading FLOAT,
            version INTEGER,
            processed_at TIMESTAMP,
            UNIQUE(timestamp, icao)
        );
        """
        pg_hook.run(create_table_sql)

        if not df.empty:
            # Convert timestamp columns
            df['timestamp'] = pd.to_datetime(df['timestamp'], unit='s')
            df['processed_at'] = pd.to_datetime(df['processed_at'])

            # Insert data using COPY command for better performance
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    # Create temp table
                    cur.execute("""
                    CREATE TEMP TABLE temp_readsb_data (LIKE readsb_aircraft_data INCLUDING DEFAULTS)
                    ON COMMIT DROP;
                    """)

                    # Copy data to temp table
                    from io import StringIO
                    output = StringIO()
                    df.to_csv(output, sep='\t', header=False, index=False)
                    output.seek(0)
                    cur.copy_from(output, 'temp_readsb_data')

                    # Insert into main table, ignoring duplicates
                    cur.execute("""
                    INSERT INTO readsb_aircraft_data
                    SELECT * FROM temp_readsb_data
                    ON CONFLICT (timestamp, icao) DO NOTHING;
                    """)

                    conn.commit()

            logging.info(f"Successfully loaded {len(df)} records into PostgreSQL")

    except Exception as e:
        logging.error(f"Error loading data to PostgreSQL: {str(e)}")
        raise

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'readsb_hist_dag',
    default_args=default_args,
    description='Downloads and processes historical readsb data',
    schedule_interval='@daily',
    start_date=datetime(2023, 11, 1),
    end_date=datetime(2024, 11, 1),
    catchup=True,
    tags=['readsb'],
) as dag:

    download_task = PythonOperator(
        task_id='download_readsb_data',
        python_callable=download_readsb_data,
        provide_context=True,
    )

    process_task = PythonOperator(
        task_id='process_readsb_data',
        python_callable=process_readsb_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True,
    )

    # Set task dependencies
    download_task >> process_task >> load_task
