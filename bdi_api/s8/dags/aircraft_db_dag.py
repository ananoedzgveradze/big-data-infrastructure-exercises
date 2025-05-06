import gzip
import json
import logging
from datetime import timedelta
from io import BytesIO

import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models import DAG, Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# Get bucket name from Airflow Variables
S3_BUCKET = Variable.get('S3_BUCKET_NAME', 'bdi-aircraft-anano')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'aircraft_db_dag',
    default_args=default_args,
    description='Processes raw aircraft database from S3 and loads it into PostgreSQL',
    schedule_interval=timedelta(days=1),
    tags=['aircraft'],
)

def process_aircraft_db(**context) -> None:
    """Process aircraft database and load into PostgreSQL"""
    s3_hook = S3Hook(aws_conn_id='aws_default')
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    try:
        # Get data from S3
        raw_key = "raw/aircraft/basic-ac-db.json.gz"
        obj = s3_hook.get_key(key=raw_key, bucket_name=S3_BUCKET)
        if not obj:
            raise Exception(f"Raw data file not found: {raw_key}")

        # Create table if not exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS aircraft_db (
            icao VARCHAR(255) PRIMARY KEY,
            registration VARCHAR(255),
            manufacturer VARCHAR(255),
            model VARCHAR(255),
            type_code VARCHAR(255),
            serial_number VARCHAR(255),
            line_number VARCHAR(255),
            icao_aircraft_type VARCHAR(255),
            operator VARCHAR(255),
            operator_callsign VARCHAR(255),
            operator_icao VARCHAR(255),
            operator_iata VARCHAR(255),
            owner VARCHAR(255),
            test_reg BOOLEAN,
            registered DATE,
            reg_until DATE,
            status VARCHAR(255),
            built DATE,
            first_flight_date DATE,
            category VARCHAR(255),
            engines VARCHAR(255),
            engine_type VARCHAR(255),
            engine_manufacturer VARCHAR(255),
            engine_model VARCHAR(255),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        pg_hook.run(create_table_sql)
        pg_hook.run("TRUNCATE TABLE aircraft_db;")

        # Process data in chunks
        CHUNK_SIZE = 1000
        processed_records = 0
        skipped_count = 0
        current_chunk = []

        with gzip.GzipFile(fileobj=BytesIO(obj.get()['Body'].read())) as gz:
            for line in gz:
                try:
                    aircraft = json.loads(line.decode('utf-8').strip())

                    if processed_records == 0:
                        logging.info(f"Sample aircraft record: {json.dumps(aircraft, indent=2)}")

                    icao = aircraft.get('icao24') or aircraft.get('icao')
                    if not icao:
                        skipped_count += 1
                        continue

                    processed_data = {
                        'icao': icao,
                        'registration': aircraft.get('registration'),
                        'manufacturer': aircraft.get('manufacturername'),
                        'model': aircraft.get('model'),
                        'type_code': aircraft.get('typecode'),
                        'serial_number': aircraft.get('serialnumber'),
                        'line_number': aircraft.get('linenumber'),
                        'icao_aircraft_type': aircraft.get('icaoaircrafttype'),
                        'operator': aircraft.get('operator'),
                        'operator_callsign': aircraft.get('operatorcallsign'),
                        'operator_icao': aircraft.get('operatoricao'),
                        'operator_iata': aircraft.get('operatoriata'),
                        'owner': aircraft.get('owner'),
                        'test_reg': aircraft.get('testreg') == 'Y',
                        'registered': None if not aircraft.get('registered') else aircraft.get('registered'),
                        'reg_until': None if not aircraft.get('reguntil') else aircraft.get('reguntil'),
                        'status': aircraft.get('status'),
                        'built': None if not aircraft.get('built') else aircraft.get('built'),
                        'first_flight_date': None if not aircraft.get('firstflightdate') else aircraft.get('firstflightdate'),
                        'category': aircraft.get('category'),
                        'engines': aircraft.get('engines'),
                        'engine_type': aircraft.get('enginetype'),
                        'engine_manufacturer': aircraft.get('enginemanufacturer'),
                        'engine_model': aircraft.get('enginemodel')
                    }

                    current_chunk.append(processed_data)
                    processed_records += 1

                    # Process chunk when it reaches CHUNK_SIZE
                    if len(current_chunk) >= CHUNK_SIZE:
                        df = pd.DataFrame(current_chunk)
                        rows_to_insert = []
                        for _, row in df.iterrows():
                            registered = pd.to_datetime(row['registered']).date() if pd.notna(row['registered']) else None
                            reg_until = pd.to_datetime(row['reg_until']).date() if pd.notna(row['reg_until']) else None
                            built = pd.to_datetime(row['built']).date() if pd.notna(row['built']) else None
                            first_flight_date = pd.to_datetime(row['first_flight_date']).date() if pd.notna(row['first_flight_date']) else None

                            rows_to_insert.append((
                                row['icao'], row['registration'], row['manufacturer'], row['model'],
                                row['type_code'], row['serial_number'], row['line_number'],
                                row['icao_aircraft_type'], row['operator'], row['operator_callsign'],
                                row['operator_icao'], row['operator_iata'], row['owner'],
                                row['test_reg'], registered, reg_until, row['status'],
                                built, first_flight_date, row['category'], row['engines'],
                                row['engine_type'], row['engine_manufacturer'], row['engine_model']
                            ))

                        pg_hook.insert_rows(
                            table='aircraft_db',
                            rows=rows_to_insert,
                            target_fields=[
                                'icao', 'registration', 'manufacturer', 'model',
                                'type_code', 'serial_number', 'line_number',
                                'icao_aircraft_type', 'operator', 'operator_callsign',
                                'operator_icao', 'operator_iata', 'owner', 'test_reg',
                                'registered', 'reg_until', 'status', 'built',
                                'first_flight_date', 'category', 'engines',
                                'engine_type', 'engine_manufacturer', 'engine_model'
                            ]
                        )
                        logging.info(f"Inserted {len(rows_to_insert)} records into aircraft_db")
                        current_chunk = []

                except json.JSONDecodeError as e:
                    logging.warning(f"Skipping invalid JSON line: {e}")
                    skipped_count += 1
                    continue

        # Process remaining records
        if current_chunk:
            df = pd.DataFrame(current_chunk)
            rows_to_insert = []
            for _, row in df.iterrows():
                registered = pd.to_datetime(row['registered']).date() if pd.notna(row['registered']) else None
                reg_until = pd.to_datetime(row['reg_until']).date() if pd.notna(row['reg_until']) else None
                built = pd.to_datetime(row['built']).date() if pd.notna(row['built']) else None
                first_flight_date = pd.to_datetime(row['first_flight_date']).date() if pd.notna(row['first_flight_date']) else None

                rows_to_insert.append((
                    row['icao'], row['registration'], row['manufacturer'], row['model'],
                    row['type_code'], row['serial_number'], row['line_number'],
                    row['icao_aircraft_type'], row['operator'], row['operator_callsign'],
                    row['operator_icao'], row['operator_iata'], row['owner'],
                    row['test_reg'], registered, reg_until, row['status'],
                    built, first_flight_date, row['category'], row['engines'],
                    row['engine_type'], row['engine_manufacturer'], row['engine_model']
                ))

            pg_hook.insert_rows(
                table='aircraft_db',
                rows=rows_to_insert,
                target_fields=[
                    'icao', 'registration', 'manufacturer', 'model',
                    'type_code', 'serial_number', 'line_number',
                    'icao_aircraft_type', 'operator', 'operator_callsign',
                    'operator_icao', 'operator_iata', 'owner', 'test_reg',
                    'registered', 'reg_until', 'status', 'built',
                    'first_flight_date', 'category', 'engines',
                    'engine_type', 'engine_manufacturer', 'engine_model'
                ]
            )
            logging.info(f"Inserted final {len(rows_to_insert)} records into aircraft_db")

        logging.info(f"Processed {processed_records} aircraft records, skipped {skipped_count} invalid records")

    except Exception as e:
        logging.error(f"Error processing aircraft data: {str(e)}")
        raise

# Create task
process_task = PythonOperator(
    task_id='process_aircraft_db',
    python_callable=process_aircraft_db,
    provide_context=True,
    dag=dag,
)
