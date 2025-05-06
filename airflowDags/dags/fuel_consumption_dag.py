import json
import logging
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Get bucket name from Airflow Variables
S3_BUCKET = Variable.get('S3_BUCKET_NAME', 'bdi-aircraft-anano')
SOURCE_URL = "https://raw.githubusercontent.com/martsec/flight_co2_analysis/main/data/aircraft_type_fuel_consumption_rates.json"

def download_fuel_data(**context) -> None:
    """Downloads aircraft fuel consumption data and stores raw data in S3."""
    s3_hook = S3Hook(aws_conn_id='aws_default')

    try:
        response = requests.get(SOURCE_URL)
        response.raise_for_status()
        data = response.json()

        try:
            s3_hook.list_keys(bucket_name=S3_BUCKET, prefix='raw/fuel', delimiter='/')
        except Exception as e:
            logging.error(f"Error accessing S3 bucket {S3_BUCKET}: {str(e)}")
            raise

        raw_key = "raw/fuel/consumption_rates.json"
        s3_hook.load_string(
            json.dumps(data),
            key=raw_key,
            bucket_name=S3_BUCKET,
            replace=True
        )

        context['task_instance'].xcom_push(key='raw_key', value=raw_key)
        logging.info(f"Successfully downloaded and stored {len(data)} records")

    except Exception as e:
        logging.error(f"Error downloading fuel consumption data: {str(e)}")
        raise

def transform_fuel_data(**context) -> None:
    """Transforms raw fuel consumption data into a structured format."""
    s3_hook = S3Hook(aws_conn_id='aws_default')

    try:
        raw_key = context['task_instance'].xcom_pull(key='raw_key', task_ids='download_fuel_data')
        obj = s3_hook.get_key(key=raw_key, bucket_name=S3_BUCKET)
        if not obj:
            raise Exception(f"Raw data file not found: {raw_key}")

        data = json.loads(obj.get()['Body'].read().decode('utf-8'))

        processed_data = []
        for aircraft_type, details in data.items():
            processed_data.append({
                'aircraft_type': aircraft_type,
                'name': details.get('name'),
                'fuel_consumption_rate': details.get('galph'),  # gallons per hour
                'category': details.get('category'),
                'source': details.get('source')
            })

        df = pd.DataFrame(processed_data)

        prepared_key = "prepared/fuel/consumption_rates.json"
        s3_hook.load_string(
            df.to_json(orient='records'),
            key=prepared_key,
            bucket_name=S3_BUCKET,
            replace=True
        )

        context['task_instance'].xcom_push(key='prepared_key', value=prepared_key)
        logging.info(f"Successfully transformed and stored {len(processed_data)} records")

    except Exception as e:
        logging.error(f"Error transforming fuel consumption data: {str(e)}")
        raise

def load_to_postgres(**context) -> None:
    """Loads transformed fuel consumption data into PostgreSQL."""
    s3_hook = S3Hook(aws_conn_id='aws_default')
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')

    try:
        prepared_key = context['task_instance'].xcom_pull(key='prepared_key', task_ids='transform_fuel_data')
        obj = s3_hook.get_key(key=prepared_key, bucket_name=S3_BUCKET)
        if not obj:
            raise Exception(f"Prepared data file not found: {prepared_key}")

        data = json.loads(obj.get()['Body'].read().decode('utf-8'))
        df = pd.DataFrame(data)

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS aircraft_fuel_consumption (
            aircraft_type VARCHAR(255) PRIMARY KEY,
            name VARCHAR(255),
            fuel_consumption_rate FLOAT,
            category VARCHAR(255),
            source VARCHAR(255),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        pg_hook.run(create_table_sql)
        pg_hook.run("TRUNCATE TABLE aircraft_fuel_consumption;")

        if not df.empty:
            pg_hook.insert_rows(
                table='aircraft_fuel_consumption',
                rows=[(
                    row['aircraft_type'],
                    row['name'],
                    row['fuel_consumption_rate'],
                    row['category'],
                    row['source']
                ) for _, row in df.iterrows()],
                target_fields=[
                    'aircraft_type',
                    'name',
                    'fuel_consumption_rate',
                    'category',
                    'source'
                ]
            )
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
    'fuel_consumption_dag',
    default_args=default_args,
    description='Downloads and processes aircraft fuel consumption data',
    schedule_interval='@daily',
    start_date=datetime(2023, 11, 1),
    catchup=False,
    tags=['fuel_consumption'],
) as dag:

    download_task = PythonOperator(
        task_id='download_fuel_data',
        python_callable=download_fuel_data,
        provide_context=True,
    )

    transform_task = PythonOperator(
        task_id='transform_fuel_data',
        python_callable=transform_fuel_data,
        provide_context=True,
    )

    load_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=load_to_postgres,
        provide_context=True,
    )

    # Set task dependencies
    download_task >> transform_task >> load_task
