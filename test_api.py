import pytest
from fastapi.testclient import TestClient
from app.main import app
import io

client = TestClient(app)


def test_health_check():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json()["status"] == "online"


def test_upload_csv():
    # Create sample CSV
    csv_content = "name,age,city\nJohn,30,NYC\nJane,25,LA"
    files = {"file": ("test.csv", io.BytesIO(csv_content.encode()), "text/csv")}

    response = client.post("/upload-csv/", files=files)
    assert response.status_code == 200
    assert response.json()["status"] == "success"
