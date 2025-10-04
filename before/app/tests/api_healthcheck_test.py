import traceback
from fastapi import HTTPException
from fastapi.testclient import TestClient
from fastapi import FastAPI
from fastapi_celery.routers.api_healthcheck import router as healthcheck_router

# Create a test FastAPI app and include the healthcheck router
app = FastAPI()
app.include_router(healthcheck_router)

client = TestClient(app)

def test_healthz():
    # Test the health check route for a successful response
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

def test_healthz_error_handling(monkeypatch):
    # Simulate an error in the health check endpoint to test error handling
    def mock_health_check_error():
        raise HTTPException(
            status_code = 503,
            detail = "Simulated error"
        )

    # Patch the healthz function to simulate the error
    monkeypatch.setattr(
        "fastapi_celery.routers.api_healthcheck._internal_health_check",
        mock_health_check_error
    )

    # Test that the error is handled properly
    response = client.get("/healthz")
    
    # Ensure that the status code is 503 and that the response contains the error details
    assert response.status_code == 503
    assert response.json()["status"] == "error"
    assert "Simulated error" in response.json()["details"]
