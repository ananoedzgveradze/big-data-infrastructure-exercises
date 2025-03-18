import pytest
from fastapi.testclient import TestClient
from bdi_api.app import app
from bdi_api.s7.exercise import connect_to_database, create_database_tables
import psycopg2
import time

client = TestClient(app)

@pytest.fixture(scope="session")
def db_connection():
    """Create a database connection for testing."""
    conn = connect_to_database()
    yield conn
    conn.close()

def test_database_connection(db_connection):
    """Test that we can connect to the database."""
    assert db_connection is not None
    assert not db_connection.closed

def test_table_creation(db_connection):
    """Test that tables are created correctly."""
    cur = db_connection.cursor()
    cur.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    tables = [row[0] for row in cur.fetchall()]
    assert 'aircraft' in tables
    assert 'aircraft_positions' in tables
    cur.close()

def test_aircraft_endpoint():
    """Test the aircraft listing endpoint."""
    response = client.get("/api/s7/aircraft/")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, list)

def test_aircraft_positions_endpoint():
    """Test the aircraft positions endpoint."""
    # First get a valid ICAO from the aircraft list
    response = client.get("/api/s7/aircraft/")
    assert response.status_code == 200
    aircraft_list = response.json()
    
    if aircraft_list:
        icao = aircraft_list[0]["icao"]
        response = client.get(f"/api/s7/aircraft/{icao}/positions")
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)

def test_aircraft_stats_endpoint():
    """Test the aircraft statistics endpoint."""
    # First get a valid ICAO from the aircraft list
    response = client.get("/api/s7/aircraft/")
    assert response.status_code == 200
    aircraft_list = response.json()
    
    if aircraft_list:
        icao = aircraft_list[0]["icao"]
        response = client.get(f"/api/s7/aircraft/{icao}/stats")
        assert response.status_code == 200
        data = response.json()
        assert "max_altitude_baro" in data
        assert "max_ground_speed" in data
        assert "had_emergency" in data

def test_prepare_endpoint():
    """Test the prepare endpoint."""
    response = client.post("/api/s7/aircraft/prepare")
    assert response.status_code == 200
    assert "Processed" in response.text 

def test_stats_endpoint_performance():
    """Test that the stats endpoint responds within 20ms."""
    response = client.get("/api/s7/aircraft/")
    assert response.status_code == 200
    aircraft_list = response.json()
    
    if aircraft_list:
        icao = aircraft_list[0]["icao"]
        
        start_time = time.time()
        response = client.get(f"/api/s7/aircraft/{icao}/stats")
        end_time = time.time()
        
        assert response.status_code == 200
        response_time = (end_time - start_time) * 1000  # Convert to milliseconds
        assert response_time < 20, f"Response time was {response_time}ms, should be < 20ms" 