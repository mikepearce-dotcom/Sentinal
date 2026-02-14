from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routes import auth, games, scans
from .database import connect_to_mongo, close_mongo_connection

app = FastAPI(title="Sentient Tracker API")

# CORS settings can be overridden via environment variable
origins = []

from os import environ
if "CORS_ORIGINS" in environ:
    origins = [o.strip() for o in environ["CORS_ORIGINS"].split(",") if o.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins or ["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# event handlers for DB
app.add_event_handler("startup", connect_to_mongo)
app.add_event_handler("shutdown", close_mongo_connection)

# include routers
app.include_router(auth.router, prefix="/api/auth", tags=["auth"])
app.include_router(games.router, prefix="/api/games", tags=["games"])
app.include_router(scans.router, prefix="/api/games", tags=["scans"])
