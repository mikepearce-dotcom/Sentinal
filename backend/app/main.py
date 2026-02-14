from os import environ
from typing import Dict, List, Optional
from urllib.parse import urlsplit

from fastapi import FastAPI, Request, Response

from .database import close_mongo_connection, connect_to_mongo
from .routes import auth, games, scans

app = FastAPI(title="Sentient Tracker API")

ALLOWED_METHODS = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
ALLOWED_HEADERS = "Content-Type, Authorization"


def _normalize_origin(value: Optional[str]) -> str:
    if not value:
        return ""

    raw = value.strip().strip('"').strip("'").rstrip("/")
    if not raw:
        return ""

    parsed = urlsplit(raw)
    if not parsed.scheme or not parsed.hostname:
        return ""

    scheme = parsed.scheme.lower()
    host = parsed.hostname.lower()
    port = parsed.port

    if (scheme == "https" and (port is None or port == 443)) or (
        scheme == "http" and (port is None or port == 80)
    ):
        return "{}://{}".format(scheme, host)

    return "{}://{}:{}".format(scheme, host, port)


def _parse_cors_origins(raw_value: Optional[str]) -> List[str]:
    if not raw_value:
        return []

    origins: List[str] = []
    for item in raw_value.split(","):
        normalized = _normalize_origin(item)
        if normalized and normalized not in origins:
            origins.append(normalized)
    return origins


def _build_cors_headers(origin: str) -> Dict[str, str]:
    return {
        "Access-Control-Allow-Origin": origin,
        "Access-Control-Allow-Credentials": "true",
        "Access-Control-Allow-Methods": ALLOWED_METHODS,
        "Access-Control-Allow-Headers": ALLOWED_HEADERS,
        "Vary": "Origin",
    }


configured_origins = _parse_cors_origins(environ.get("CORS_ORIGINS"))
default_origins = [
    "http://localhost:3000",
    "https://noble-radiance-production.up.railway.app",
]
allowed_origins = configured_origins or default_origins
allowed_origin_set = set(allowed_origins)

print("CORS allowlist: {}".format(allowed_origins))


@app.middleware("http")
async def cors_middleware(request: Request, call_next):
    request_origin = request.headers.get("origin", "")
    normalized_request_origin = _normalize_origin(request_origin)
    is_allowed_origin = normalized_request_origin in allowed_origin_set

    if request.method == "OPTIONS":
        if not is_allowed_origin:
            return Response(status_code=403)
        return Response(status_code=204, headers=_build_cors_headers(request_origin))

    response = await call_next(request)

    if is_allowed_origin:
        headers = _build_cors_headers(request_origin)
        for key, value in headers.items():
            response.headers[key] = value

    return response


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
