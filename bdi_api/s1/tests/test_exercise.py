from fastapi.testclient import TestClient
from bdi_api.app import app  # Import FastAPI app

client = TestClient(app)

def test_list_aircraft():
    response = client.get("/api/s1/aircraft/")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

