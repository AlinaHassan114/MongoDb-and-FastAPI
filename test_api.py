import pytest
from fastapi.testclient import TestClient
import sys
import os


sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'app'))

from main import app

client = TestClient(app)

def test_health_check():
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_metrics():
    response = client.get("/metrics")
    assert response.status_code == 200
    data = response.json()
    assert "collections" in data
    assert "books" in data["collections"]

def test_list_books():
    response = client.get("/books?page=1&page_size=5")
    assert response.status_code == 200
    data = response.json()
    assert "items" in data
    assert "page" in data
    assert "total" in data

def test_get_book():
    response = client.get("/books/1")
    assert response.status_code == 200
    data = response.json()
    assert "book_id" in data

def test_get_book_tags():
    response = client.get("/books/1/tags")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_get_author_books():
    response = client.get("/authors/Suzanne/books")
    assert response.status_code == 200
    data = response.json()
    assert "items" in data

def test_list_tags():
    response = client.get("/tags")
    assert response.status_code == 200
    data = response.json()
    assert "items" in data

def test_get_user_to_read():
    response = client.get("/users/1/to-read")
    assert response.status_code == 200
    assert isinstance(response.json(), list)

def test_get_ratings_summary():
    response = client.get("/books/1/ratings/summary")
    assert response.status_code == 200
    data = response.json()
    assert "average_rating" in data
    assert "histogram" in data

def test_create_rating_with_auth():
    rating_data = {
        "user_id": 999,
        "book_id": 1,
        "rating": 5
    }
    response = client.post(
        "/ratings", 
        json=rating_data,
        headers={"x-api-key": "dev-key-123"}
    )
    assert response.status_code in [200, 201]

def test_get_recommendations():
    response = client.get("/users/1/recommendations?top_k=5")
    assert response.status_code == 200
    data = response.json()
    assert "recommendations" in data
    assert "type" in data

def test_root_endpoint():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json()["message"] == "GoodBooks API is running"