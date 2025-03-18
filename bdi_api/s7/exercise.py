import io
import gzip
import json
import boto3
from fastapi import APIRouter, status
from bdi_api.settings import DBCredentials, Settings
import psycopg2
from psycopg2.extras import execute_batch
from psycopg2.pool import SimpleConnectionPool

settings = Settings()
db_credentials = DBCredentials()
s3_client = boto3.client("s3")
BUCKET_NAME = "bdi-aircraft-anano"
S3_PREFIX_PATH = "raw/day=20231101/"

# Add connection pooling
pool = SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    dbname=db_credentials.database,
    user=db_credentials.username,
    password=db_credentials.password,
    host=db_credentials.host,
    port=db_credentials.port
)

def connect_to_database():
    """Get a connection from the pool."""
    return pool.getconn()

def return_connection(conn):
    """Return connection to the pool."""
    if conn:
        pool.putconn(conn)

def create_database_tables():
    """Create the necessary database tables if they don't exist."""
    conn = connect_to_database()
    cur = conn.cursor()
    
    try:
        # Create aircraft table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS aircraft (
                icao VARCHAR PRIMARY KEY,
                registration VARCHAR,
                type VARCHAR
            );
        """)
        
        # Create aircraft_positions table
        cur.execute("""
            CREATE TABLE IF NOT EXISTS aircraft_positions (
                icao VARCHAR REFERENCES aircraft(icao),
                timestamp BIGINT,
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION,
                altitude_baro DOUBLE PRECISION,
                ground_speed DOUBLE PRECISION,
                emergency BOOLEAN,
                PRIMARY KEY (icao, timestamp)
            );
        """)
        
        # Create optimized indexes
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_aircraft_positions_icao_timestamp 
            ON aircraft_positions(icao, timestamp);
            
            CREATE INDEX IF NOT EXISTS idx_aircraft_positions_icao_altitude 
            ON aircraft_positions(icao, altitude_baro);
            
            CREATE INDEX IF NOT EXISTS idx_aircraft_positions_icao_speed 
            ON aircraft_positions(icao, ground_speed);
            
            CREATE INDEX IF NOT EXISTS idx_aircraft_positions_icao_emergency 
            ON aircraft_positions(icao, emergency);
        """)
        
        conn.commit()
    finally:
        cur.close()
        return_connection(conn)

def get_all_files_from_s3():
    all_data = []
    for obj in s3_client.list_objects_v2(Bucket=BUCKET_NAME).get("Contents", []):
        file_key = obj["Key"]
        file_data = get_file_from_s3(file_key)
        all_data.extend(file_data)
    return all_data

def get_file_from_s3(file_key):
    obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
    content = obj["Body"].read()
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(content)) as gz:
            data = json.loads(gz.read().decode("utf-8"))
    except:
        data = json.loads(content.decode("utf-8"))
    
    return data.get("aircraft", data) if isinstance(data, dict) else data

def save_to_database(data):
    conn = connect_to_database()
    cur = conn.cursor()
    
    aircraft_data = []
    position_data = []
    
    for record in data:
        if not isinstance(record, dict):
            continue
            
        # Get ICAO code and remove any "~" prefix
        icao = record.get("hex", "")
        if icao.startswith("~"):
            icao = icao[1:]  # Remove the "~" prefix
        if not icao:
            continue
            
        aircraft_data.append((
            icao,
            record.get("r", ""),  # registration
            record.get("t", "")   # type
        ))
        
        if "lat" in record and "lon" in record:
            # Handle altitude - convert 'ground' to 0
            alt_baro = record.get("alt_baro", 0)
            if isinstance(alt_baro, str) and alt_baro.lower() == "ground":
                alt_baro = 0
            else:
                try:
                    alt_baro = float(alt_baro or 0)
                except (ValueError, TypeError):
                    alt_baro = 0
            
            # Handle ground speed
            try:
                ground_speed = float(record.get("gs", 0) or 0)
            except (ValueError, TypeError):
                ground_speed = 0
                
            position_data.append((
                icao,
                record.get("seen", 0),
                record["lat"],
                record["lon"],
                alt_baro,
                ground_speed,
                bool(record.get("emergency", False))
            ))
    
    if aircraft_data:
        execute_batch(cur, """
            INSERT INTO aircraft (icao, registration, type)
            VALUES (%s, %s, %s)
            ON CONFLICT (icao) DO UPDATE SET
                registration = EXCLUDED.registration,
                type = EXCLUDED.type
        """, aircraft_data)
        
    if position_data:
        execute_batch(cur, """
            INSERT INTO aircraft_positions
            (icao, timestamp, lat, lon, altitude_baro, ground_speed, emergency)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (icao, timestamp) DO NOTHING
        """, position_data)
    
    conn.commit()
    cur.close()
    return_connection(conn)

def create_database():
    """Create the database if it doesn't exist."""
    try:
        # Connect to default postgres database
        conn = psycopg2.connect(
            dbname="postgres",
            user=db_credentials.username,
            password=db_credentials.password,
            host=db_credentials.host,
            port=db_credentials.port
        )
        conn.autocommit = True  # Required for creating database
        cur = conn.cursor()
        
        try:
            # Check if database exists
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_credentials.database,))
            exists = cur.fetchone()
            
            if not exists:
                cur.execute(f'CREATE DATABASE {db_credentials.database}')
                print(f"Created database {db_credentials.database}")
            else:
                print(f"Database {db_credentials.database} already exists")
        finally:
            cur.close()
            conn.close()
    except psycopg2.OperationalError as e:
        print(f"Error connecting to PostgreSQL: {str(e)}")
        print("Please make sure:")
        print("1. PostgreSQL is installed and running")
        print("2. The credentials are correct:")
        print(f"   Host: {db_credentials.host}")
        print(f"   Port: {db_credentials.port}")
        print(f"   Username: {db_credentials.username}")
        print(f"   Database: {db_credentials.database}")
        raise

BASE_URL = "https://samples.adsbexchange.com/readsb-hist/2023/11/01/"

s7 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s7",
    tags=["s7"],
)


@s7.post("/aircraft/prepare")
def prepare_data() -> str:
    """Get the raw data from s3 and insert it into RDS
    
    Use credentials passed from `db_credentials`
    """
    try:
        print("Starting prepare_data...")
        print(f"Database credentials: {db_credentials}")
        print(f"S3 bucket: {BUCKET_NAME}")
        print(f"S3 prefix path: {S3_PREFIX_PATH}")
        
        # Create database if it doesn't exist
        print("Creating database...")
        create_database()
        
        # Create database tables if they don't exist
        print("Creating database tables...")
        create_database_tables()
        
        # Initialize counters
        total_files = 0
        total_aircraft = 0
        total_positions = 0
        
        print(f"Connecting to S3 bucket: {BUCKET_NAME}")
        print(f"Looking for files in prefix: {S3_PREFIX_PATH}")
        
        # Use paginator to handle more than 1000 files
        paginator = s3_client.get_paginator('list_objects_v2')
        
        try:
            # Iterate through all pages of S3 objects with correct prefix
            for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=S3_PREFIX_PATH):
                if 'Contents' not in page:
                    print("No files found in this page")
                    continue
                    
                print(f"Found {len(page['Contents'])} files in this page")
                
                for obj in page['Contents']:
                    try:
                        print(f"Processing file: {obj['Key']}")
                        # Get file content from S3
                        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=obj['Key'])
                        file_content = response['Body'].read()
                        
                        # Process the file
                        data = get_file_from_s3(obj['Key'])
                        
                        # Save to database
                        if data:
                            save_to_database(data)
                            total_aircraft += len(data)
                            total_positions += len(data)
                        
                        total_files += 1
                        print(f"Processed file {obj['Key']}: {len(data)} aircraft, {len(data)} positions")
                        
                    except Exception as e:
                        print(f"Error processing file {obj['Key']}: {str(e)}")
                        continue
            
            if total_files == 0:
                return "No files found in S3 bucket"
                
            return f"Processed {total_files} files. Inserted {total_aircraft} aircraft and {total_positions} positions."
        
        except Exception as e:
            print(f"Error during S3 processing: {str(e)}")
            raise
        
    except Exception as e:
        print(f"Error in prepare_data: {str(e)}")
        print(f"Error type: {type(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        raise


@s7.get("/aircraft/")
def list_aircraft(num_results: int = 100, page: int = 0) -> list[dict]:
    """List all the available aircraft, its registration and type ordered by
    icao asc FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    conn = connect_to_database()
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT icao, registration, type FROM aircraft ORDER BY icao LIMIT %s OFFSET %s", (num_results, page * num_results))
        
        results = [
            {
                "icao": row[0],
                "registration": row[1],
                "type": row[2]
            }
            for row in cur.fetchall()
        ]
        
        return results
    finally:
        cur.close()
        return_connection(conn)


@s7.get("/aircraft/{icao}/positions")
def get_aircraft_position(icao: str, num_results: int = 1000, page: int = 0) -> list[dict]:
    """Returns all the known positions of an aircraft ordered by time (asc)
    If an aircraft is not found, return an empty list. FROM THE DATABASE

    Use credentials passed from `db_credentials`
    """
    conn = connect_to_database()
    
    try:
        cur = conn.cursor()
        cur.execute("SELECT timestamp, lat, lon FROM aircraft_positions WHERE icao = %s ORDER BY timestamp ASC LIMIT %s OFFSET %s", (icao, num_results, page * num_results))
        
        results = [
            {
                "timestamp": row[0],
                "lat": row[1],
                "lon": row[2]
            }
            for row in cur.fetchall()
        ]
        
        return results
    finally:
        cur.close()
        return_connection(conn)


@s7.get("/aircraft/{icao}/stats")
def get_aircraft_statistics(icao: str) -> dict:
    """Returns different statistics about the aircraft"""
    conn = connect_to_database()
    
    try:
        cur = conn.cursor()
        cur.execute("""
            WITH stats AS (
                SELECT 
                    MAX(altitude_baro) as max_alt,
                    MAX(ground_speed) as max_speed,
                    BOOL_OR(emergency) as had_emergency
                FROM aircraft_positions
                WHERE icao = %s
            )
            SELECT 
                COALESCE(max_alt, 0),
                COALESCE(max_speed, 0),
                COALESCE(had_emergency, FALSE)
            FROM stats
        """, (icao,))
        
        row = cur.fetchone()
        return {
            "max_altitude_baro": float(row[0]),
            "max_ground_speed": float(row[1]),
            "had_emergency": bool(row[2])
        }
    finally:
        cur.close()
        return_connection(conn)
