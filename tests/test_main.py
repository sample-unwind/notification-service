from fastapi.testclient import TestClient

from main import app

client = TestClient(app)


def test_health_live():
    response = client.get("/health/live")
    assert response.status_code == 200
    assert response.json() == {"status": "alive"}


def test_health_ready():
    response = client.get("/health/ready")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ready"
    assert data["rabbitmq"] == "connected"


def test_root():
    response = client.get("/")
    assert response.status_code == 200
    data = response.json()
    assert data["service"] == "Notification Service"
    assert "description" in data
    assert "endpoints" in data
    assert "rabbitmq" in data


def test_events_schema():
    response = client.get("/events/schema")
    assert response.status_code == 200
    data = response.json()
    assert "events" in data
    assert "payment.processed" in data["events"]
    assert "schema" in data["events"]["payment.processed"]
