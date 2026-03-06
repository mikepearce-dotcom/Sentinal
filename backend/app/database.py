import os
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient

client: Optional[AsyncIOMotorClient] = None
db = None


async def _ensure_indexes() -> None:
    if db is None:
        return

    await db.users.create_index("user_id", unique=True, name="uniq_users_user_id")
    await db.users.create_index("email", unique=True, name="uniq_users_email")
    await db.users.create_index(
        "auth0_sub",
        unique=True,
        sparse=True,
        name="uniq_users_auth0_sub",
    )

    await db.tracked_games.create_index([("user_id", 1), ("name", 1)], name="idx_tracked_games_user_name")

    await db.scan_results.create_index(
        [("game_id", 1), ("user_id", 1), ("created_at", -1)],
        name="idx_scan_results_game_user_created",
    )

    await db.community_games.create_index("slug", unique=True, name="uniq_community_games_slug")
    await db.community_games.create_index("normalized_name", name="idx_community_games_normalized_name")

    await db.community_petitions.create_index("slug", unique=True, name="uniq_community_petitions_slug")
    await db.community_petitions.create_index(
        [("status", 1), ("last_support_at", -1), ("supporter_count", -1), ("created_at", -1)],
        name="idx_community_petitions_status_momentum",
    )
    await db.community_petitions.create_index(
        [("created_by_user_id", 1), ("created_at", -1)],
        name="idx_community_petitions_creator_created",
    )

    await db.community_petition_signatures.create_index(
        [("petition_id", 1), ("user_id", 1)],
        unique=True,
        name="uniq_community_signatures_petition_user",
    )
    await db.community_petition_signatures.create_index(
        [("petition_id", 1), ("created_at", -1)],
        name="idx_community_signatures_petition_created",
    )


async def connect_to_mongo():
    global client, db
    mongo_url = os.getenv("MONGO_URL", "mongodb://localhost:27017")
    db_name = os.getenv("DB_NAME", "sentient_tracker")
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    await _ensure_indexes()
    print("Connected to MongoDB")


async def close_mongo_connection():
    global client
    if client:
        client.close()
        print("Closed MongoDB connection")
