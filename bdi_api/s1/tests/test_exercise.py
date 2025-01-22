from fastapi.testclient import TestClient

from bdi_api.app import app  # Import FastAPI app

client = TestClient(app)

# Test the /api/s1/aircraft endpoint
def test_list_aircraft():
    response = client.get("/api/s1/aircraft/?num_results=10&page=0")
    assert response.status_code == 200
    assert isinstance(response.json(), list)
    assert len(response.json()) == 10  # or whatever number of results you expect

# Test the /api/s1/aircraft/{icao}/stats endpoint
def test_get_aircraft_stats():
    response = client.get("/api/s1/aircraft/a65800/stats")
    assert response.status_code == 200
    stats = response.json()
    assert "max_altitude_baro" in stats
    assert "max_ground_speed" in stats
    assert "had_emergency" in stats

# Test the /api/s1/aircraft/download endpoint
def test_download_data():
    response = client.post("/api/s1/aircraft/download", params={"file_limit": 5})
    assert response.status_code == 200
    assert response.text == "OK"

# Test the /api/s1/aircraft/prepare endpoint
def test_prepare_data():
    response = client.post("/api/s1/aircraft/prepare")
    assert response.status_code == 200
    assert response.text == "OK"

# Test for error handling when trying to fetch a non-existent aircraft
def test_get_aircraft_stats_not_found():
    response = client.get("/api/s1/aircraft/nonexistent_icao/stats")
    assert response.status_code == 404
    assert "detail" in response.json()

# Test for error handling when trying to download files but without the right parameters
def test_download_data_invalid_limit():
    response = client.post("/api/s1/aircraft/download", params={"file_limit": -1})
    assert response.status_code == 422  # Unprocessable Entity for invalid parameter

# Test the /api/s1/aircraft/{icao}/positions endpoint
def test_get_aircraft_positions():
    response = client.get("/api/s1/aircraft/a65800/positions?num_results=5&page=0")
    assert response.status_code == 200
    positions = response.json()
    assert isinstance(positions, list)
    assert len(positions) == 5

# Test the /api/s1/aircraft/ endpoint with an empty response
def test_list_aircraft_empty():
    response = client.get("/api/s1/aircraft/?num_results=0&page=0")
    assert response.status_code == 200
    assert len(response.json()) == 0

