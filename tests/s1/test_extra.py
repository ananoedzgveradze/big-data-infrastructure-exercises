import pytest
from fastapi.testclient import TestClient

# Import the app from the correct module
from bdi_api.app import app

client = TestClient(app)

def test_health_endpoint():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == "ok" or response.json() == {"message": "ok"}

def test_version_endpoint():
    response = client.get("/version")
    assert response.status_code == 200
    assert "version" in response.json()

def test_root_endpoint():
    response = client.get("/")
    assert response.status_code == 200
    assert "Welcome to the Aircraft API" in response.json().get("message", "")
