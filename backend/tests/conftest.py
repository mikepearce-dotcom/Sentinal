import pytest
from fastapi.testclient import TestClient
from app.main import app
from app import database


@pytest.fixture
def client():
    # startup will connect to mongo; then we drop collections for a clean state
    with TestClient(app) as c:
        # drop any existing data
        import asyncio
        db = database.db
        if db:
            asyncio.run(db.users.delete_many({}))
            asyncio.run(db.tracked_games.delete_many({}))
            asyncio.run(db.scan_results.delete_many({}))
        yield c
