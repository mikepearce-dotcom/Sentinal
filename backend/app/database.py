import os
from typing import Any, Optional

from motor.motor_asyncio import AsyncIOMotorClient

client: Optional[AsyncIOMotorClient] = None
db = None


def _env_truthy(value: Optional[str], default: bool = True) -> bool:
    cleaned = str(value or "").strip().lower()
    if not cleaned:
        return default
    return cleaned in {"1", "true", "yes", "on"}


async def _safe_create_index(collection: Any, keys: Any, **kwargs: Any) -> None:
    index_name = str(kwargs.get("name") or keys)
    try:
        await collection.create_index(keys, **kwargs)
    except Exception as exc:
        # Non-fatal by design: existing duplicate data can block new unique indexes.
        print(f"Index create skipped ({index_name}): {type(exc).__name__}: {exc}")


async def _ensure_indexes() -> None:
    if db is None:
        return

    await _safe_create_index(db.users, "user_id", unique=True, name="uniq_users_user_id")
    await _safe_create_index(db.users, "email", unique=True, name="uniq_users_email")
    await _safe_create_index(
        db.users,
        "auth0_sub",
        unique=True,
        sparse=True,
        name="uniq_users_auth0_sub",
    )

    await _safe_create_index(db.tracked_games, [("user_id", 1), ("name", 1)], name="idx_tracked_games_user_name")

    await _safe_create_index(
        db.scan_results,
        [("game_id", 1), ("user_id", 1), ("created_at", -1)],
        name="idx_scan_results_game_user_created",
    )

    await _safe_create_index(db.community_games, "slug", unique=True, name="uniq_community_games_slug")
    await _safe_create_index(db.community_games, "normalized_name", name="idx_community_games_normalized_name")

    await _safe_create_index(db.community_petitions, "slug", unique=True, name="uniq_community_petitions_slug")
    await _safe_create_index(
        db.community_petitions,
        [("status", 1), ("last_support_at", -1), ("supporter_count", -1), ("created_at", -1)],
        name="idx_community_petitions_status_momentum",
    )
    await _safe_create_index(
        db.community_petitions,
        [("created_by_user_id", 1), ("created_at", -1)],
        name="idx_community_petitions_creator_created",
    )

    await _safe_create_index(
        db.community_petition_signatures,
        [("petition_id", 1), ("user_id", 1)],
        unique=True,
        name="uniq_community_signatures_petition_user",
    )
    await _safe_create_index(
        db.community_petition_signatures,
        [("petition_id", 1), ("created_at", -1)],
        name="idx_community_signatures_petition_created",
    )


async def connect_to_mongo():
    global client, db
    mongo_url = os.getenv("MONGO_URL", "mongodb://localhost:27017")
    db_name = os.getenv("DB_NAME", "sentient_tracker")
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]

    if _env_truthy(os.getenv("DB_AUTO_INDEXES"), default=True):
        await _ensure_indexes()

    print("Connected to MongoDB")


async def close_mongo_connection():
    global client
    if client:
        client.close()
        print("Closed MongoDB connection")
