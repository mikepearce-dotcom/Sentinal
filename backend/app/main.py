from os import environ

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .database import close_mongo_connection, connect_to_mongo
from .routes import auth, games, scans


app = FastAPI(title="Sentient Tracker API")


def _parse_cors_origins(raw_value: str) -> list[str]:
    origins = []
    for item in raw_value.split(","):
        origin = item.strip().strip('"').strip("'").rstrip("/")
        if origin:
            origins.append(origin)
    return origins


origins = _parse_cors_origins(environ.get("CORS_ORIGINS", ""))
origin_regex = environ.get("CORS_ORIGIN_REGEX", "").strip() or None

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins or ["*"],
    allow_origin_regex=origin_regex,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health")
async def health():
    return {"status": "ok"}


# event handlers for DB
app.add_event_handler("startup", connect_to_mongo)
app.add_event_handler("shutdown", close_mongo_connection)

# include routers
app.include_router(auth.router, prefix="/api/auth", tags=["auth"])
app.include_router(games.router, prefix="/api/games", tags=["games"])
app.include_router(scans.router, prefix="/api/games", tags=["scans"])
