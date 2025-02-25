from fastapi.testclient import TestClient

from bdi_api.app import app

client = TestClient(app)


class TestS4Endpoints:
    """
    Tests for the /api/s4 endpoints: download and prepare.
    """

    def test_download_aircraft(self):
        """
        Test the /api/s4/aircraft/download endpoint.
        Ensures that aircraft data is downloaded and stored in S3.
        """
        response = client.post("/api/s4/aircraft/download", params={"file_limit": 1})
        assert response.status_code == 200
        assert "Downloaded and uploaded" in response.text

    def test_prepare_aircraft(self):
        """
        Test the /api/s4/aircraft/prepare endpoint.
        Ensures that data is retrieved from S3 and stored locally.
        """
        response = client.post("/api/s4/aircraft/prepare")
        assert response.status_code == 200
        assert "Processed" in response.text
