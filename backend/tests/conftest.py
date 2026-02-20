import asyncio

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app import database


@pytest.fixture
def client():
    # startup will connect to mongo; then we drop collections for a clean state
    with TestClient(app) as c:
        db = database.db
        if db is not None:
            loop = asyncio.new_event_loop()
            try:
                asyncio.set_event_loop(loop)

                async def _clear_collections():
                    await db.users.delete_many({})
                    await db.tracked_games.delete_many({})
                    await db.scan_results.delete_many({})

                loop.run_until_complete(_clear_collections())
            finally:
                asyncio.set_event_loop(None)
                loop.close()
        yield c
