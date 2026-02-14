import os
from typing import Optional

from motor.motor_asyncio import AsyncIOMotorClient

client: Optional[AsyncIOMotorClient] = None
db = None


def connect_to_mongo():
    global client, db
    mongo_url = os.getenv("MONGO_URL", "mongodb://localhost:27017")
    db_name = os.getenv("DB_NAME", "sentient_tracker")
    client = AsyncIOMotorClient(mongo_url)
    db = client[db_name]
    print("Connected to MongoDB")


def close_mongo_connection():
    global client
    if client:
        client.close()
        print("Closed MongoDB connection")
