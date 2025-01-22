import pytest
from httpx import AsyncClient

from bdi_api.app import app


@pytest.mark.asyncio
async def test_list_aircraft():
    """Test that the /aircraft/ endpoint returns aircraft data."""
    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get("/api/s1/aircraft/?num_results=5&page=0")

    assert response.status_code == 200
    data = response.json()

    # Ensure response contains a list
    assert isinstance(data, list)
    assert len(data) > 0  # Should return at least one aircraft

    # Validate keys in response
    assert "icao" in data[0]
    assert "flight" in data[0]
    assert "lat" in data[0]
    assert "lon" in data[0]
    assert "altitude" in data[0]
    assert "speed" in data[0]
    assert "timestamp" in data[0]
    assert "aircraft_type" in data[0]
    assert "registration" in data[0]


@pytest.mark.asyncio
async def test_aircraft_positions():
    """Test that the /aircraft/{icao}/positions endpoint returns aircraft position data."""
    test_icao = "01013c"  # Replace with a valid ICAO from your dataset

    async with AsyncClient(app=app, base_url="http://test") as ac:
        response = await ac.get(f"/api/s1/aircraft/{test_icao}/positions?num_results=5&page=0")

    assert response.status_code == 200
    data = response.json()

    assert isinstance(data, list)
    assert len(data) > 0

    assert "timestamp" in data[0]
    assert "lat" in data[0]
    assert "lon" in data[0]
