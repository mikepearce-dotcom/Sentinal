import pytest


def test_signup_and_login(client):
    # ensure clean test DB? assuming fresh environment
    response = client.post("/api/auth/signup", json={"email": "test@example.com", "name": "Tester", "password": "password123"})
    assert response.status_code == 200
    assert response.json()["message"] == "user created"

    # login
    response = client.post("/api/auth/login", json={"email": "test@example.com", "password": "password123"})
    assert response.status_code == 200
    data = response.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"
