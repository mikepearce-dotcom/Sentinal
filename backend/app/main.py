from os import environ

from fastapi import FastAPI, Response
from fastapi.middleware.cors import CORSMiddleware

from .database import close_mongo_connection, connect_to_mongo
from .routes import auth, games, scans

app = FastAPI(title="Sentient Tracker API")


def _parse_cors_origins(raw_value: str | None) -> list[str]:
    if not raw_value:
        return []

    origins: list[str] = []
    for item in raw_value.split(","):
        origin = item.strip().strip('"').strip("'").rstrip("/")
        if origin and origin not in origins:
            origins.append(origin)
    return origins


configured_origins = _parse_cors_origins(environ.get("CORS_ORIGINS"))
default_origins = [
    "http://localhost:3000",
    "https://noble-radiance-production.up.railway.app",
]
allow_origins = configured_origins or default_origins

app.add_middleware(
    CORSMiddleware,
    allow_origins=allow_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization"],
)


@app.options("/{path:path}", include_in_schema=False)
async def cors_preflight(path: str):
    return Response(status_code=204)


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
