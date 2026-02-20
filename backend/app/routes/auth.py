import os
import uuid
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, Optional

import jwt
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from pydantic import BaseModel

from .. import database
from ..models import Token, UserCreate
from ..security import allow_request, clean_env, client_ip, env_truthy, parse_int_env
from ..utils import hash_password, verify_password

router = APIRouter()

# Legacy JWT utilities (kept for local/dev compatibility when Auth0 vars are absent)
JWT_SECRET = os.getenv("JWT_SECRET", "change-me")
JWT_ALGORITHM = "HS256"
TOKEN_EXPIRE_MINUTES = 60


# Auth0 configuration

def _normalize_auth0_domain(value: Optional[str]) -> str:
    cleaned = clean_env(value).rstrip("/")
    lower = cleaned.lower()
    if lower.startswith("https://"):
        cleaned = cleaned[8:]
    elif lower.startswith("http://"):
        cleaned = cleaned[7:]
    return cleaned.strip().lower()


def _normalize_auth0_audience(value: Optional[str]) -> str:
    return clean_env(value).rstrip("/")


AUTH0_DOMAIN = _normalize_auth0_domain(os.getenv("AUTH0_DOMAIN"))
AUTH0_AUDIENCE = _normalize_auth0_audience(os.getenv("AUTH0_AUDIENCE"))
AUTH0_CLIENT_ID = clean_env(os.getenv("AUTH0_CLIENT_ID"))
AUTH0_ISSUER = f"https://{AUTH0_DOMAIN}/" if AUTH0_DOMAIN else ""
AUTH0_JWKS_URL = f"{AUTH0_ISSUER}.well-known/jwks.json" if AUTH0_ISSUER else ""

AUTH_RATE_WINDOW_SECONDS = parse_int_env(os.getenv("AUTH_RATE_WINDOW_SECONDS"), default=600)
AUTH_LOGIN_RATE_LIMIT = parse_int_env(os.getenv("AUTH_LOGIN_RATE_LIMIT"), default=60)
AUTH_SIGNUP_RATE_LIMIT = parse_int_env(os.getenv("AUTH_SIGNUP_RATE_LIMIT"), default=30)


def _auth0_enabled() -> bool:
    return bool(AUTH0_DOMAIN and AUTH0_AUDIENCE and AUTH0_ISSUER and AUTH0_JWKS_URL)


def _legacy_auth_enabled() -> bool:
    explicit = clean_env(os.getenv("ALLOW_LEGACY_AUTH"))
    if explicit:
        return env_truthy(explicit, default=False)

    running_on_railway = any(
        clean_env(os.getenv(name))
        for name in ("RAILWAY_ENVIRONMENT", "RAILWAY_PROJECT_ID", "RAILWAY_SERVICE_ID")
    )
    if running_on_railway or AUTH0_DOMAIN or AUTH0_AUDIENCE:
        # Secure default for deployed/Auth0-configured environments.
        return False

    return True


if _auth0_enabled():
    print(
        "Auth mode: auth0 "
        f"(domain={AUTH0_DOMAIN}, audience={AUTH0_AUDIENCE}, client_id={'set' if AUTH0_CLIENT_ID else 'unset'})"
    )
elif _legacy_auth_enabled():
    print("Auth mode: legacy JWT")
else:
    print("Auth mode: misconfigured (Auth0 disabled and legacy auth not allowed)")


@lru_cache(maxsize=1)
def _jwks_client() -> Optional[jwt.PyJWKClient]:
    if not _auth0_enabled():
        return None
    return jwt.PyJWKClient(AUTH0_JWKS_URL)


def _first_non_empty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _derive_name(email: str, fallback: str = "") -> str:
    if fallback.strip():
        return fallback.strip()

    email_value = (email or "").strip()
    if "@" in email_value:
        return email_value.split("@", 1)[0]

    return "Auth0 User"


def _ensure_auth_rate_limit(scope: str, request: Request, limit: int) -> None:
    key = f"auth:{scope}:{client_ip(request)}"
    if allow_request(key, limit=limit, window_seconds=AUTH_RATE_WINDOW_SECONDS):
        return

    raise HTTPException(status_code=429, detail="Too many authentication attempts. Please try again shortly.")


def create_access_token(user_id: str) -> str:
    expire = datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRE_MINUTES)
    to_encode = {"user_id": user_id, "exp": expire}
    return jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_access_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.PyJWTError:
        return {}


def _decode_auth0_access_token(token: str) -> Dict[str, Any]:
    if not _auth0_enabled():
        return {}

    try:
        jwks = _jwks_client()
        if jwks is None:
            return {}

        signing_key = jwks.get_signing_key_from_jwt(token)
        expected_audience = [AUTH0_AUDIENCE]
        if AUTH0_AUDIENCE.endswith("/"):
            expected_audience.append(AUTH0_AUDIENCE.rstrip("/"))
        else:
            expected_audience.append(f"{AUTH0_AUDIENCE}/")

        decoded = jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=expected_audience,
            issuer=AUTH0_ISSUER,
        )

        if AUTH0_CLIENT_ID:
            azp = _first_non_empty(decoded.get("azp"), decoded.get("client_id"))
            if azp != AUTH0_CLIENT_ID:
                raise jwt.InvalidTokenError("Token authorized party does not match expected client")

        return decoded
    except Exception as exc:
        print(f"Auth0 token decode failed: {type(exc).__name__}: {exc}")
        return {}


async def _get_or_create_auth0_user(claims: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    auth0_sub = _first_non_empty(claims.get("sub"))
    if not auth0_sub:
        return None

    existing = await database.db.users.find_one({"auth0_sub": auth0_sub})
    if existing:
        return existing

    email = _first_non_empty(claims.get("email"), claims.get("upn"), claims.get("preferred_username"))
    email_verified_raw = claims.get("email_verified")
    email_verified = email_verified_raw is True or str(email_verified_raw).strip().lower() in {
        "1",
        "true",
        "yes",
    }

    if email:
        by_email = await database.db.users.find_one({"email": email})
        if by_email:
            existing_sub = _first_non_empty(by_email.get("auth0_sub"))
            if existing_sub and existing_sub != auth0_sub:
                print("Auth0 link blocked: email belongs to a different Auth0 subject")
                return None

            if not email_verified:
                print("Auth0 link blocked: email is not verified")
                return None

            name = _derive_name(
                email,
                _first_non_empty(claims.get("name"), claims.get("nickname"), claims.get("given_name")),
            )
            await database.db.users.update_one(
                {"_id": by_email["_id"]},
                {
                    "$set": {
                        "auth0_sub": auth0_sub,
                        "auth_provider": "auth0",
                        "name": by_email.get("name") or name,
                    }
                },
            )
            refreshed = await database.db.users.find_one({"_id": by_email["_id"]})
            return refreshed or by_email

    # Do not trust unverified email claims as canonical identity.
    if email and not email_verified:
        email = ""

    if not email:
        safe_sub = auth0_sub.replace("|", ".").replace(" ", "")
        email = f"{safe_sub}@auth0.local"

    name = _derive_name(
        email,
        _first_non_empty(claims.get("name"), claims.get("nickname"), claims.get("given_name")),
    )

    user_doc = {
        "user_id": str(uuid.uuid4()),
        "email": email,
        "name": name,
        "auth_provider": "auth0",
        "auth0_sub": auth0_sub,
        "created_at": datetime.utcnow(),
    }
    await database.db.users.insert_one(user_doc)
    return user_doc


class LoginData(BaseModel):
    email: str
    password: str


@router.post("/signup")
async def signup(user: UserCreate, request: Request):
    _ensure_auth_rate_limit("signup", request, AUTH_SIGNUP_RATE_LIMIT)

    if not _legacy_auth_enabled():
        raise HTTPException(status_code=404, detail="Not found")

    email = str(user.email or "").strip().lower()
    existing = await database.db.users.find_one({"email": email})
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")

    user_dict = user.dict()
    user_dict["email"] = email
    user_dict["password_hash"] = hash_password(user_dict.pop("password"))
    user_dict["user_id"] = str(uuid.uuid4())
    user_dict["auth_provider"] = "email"
    user_dict["created_at"] = datetime.utcnow()

    await database.db.users.insert_one(user_dict)
    return {"message": "user created"}


@router.post("/login", response_model=Token)
async def login(data: LoginData, request: Request):
    _ensure_auth_rate_limit("login", request, AUTH_LOGIN_RATE_LIMIT)

    if not _legacy_auth_enabled():
        raise HTTPException(status_code=404, detail="Not found")

    email = str(data.email or "").strip().lower()
    user = await database.db.users.find_one({"email": email})
    if not user:
        raise HTTPException(status_code=401, detail="Invalid email or password")

    password_hash = user.get("password_hash")
    if not password_hash or not verify_password(data.password, password_hash):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    access_token = create_access_token(user_id=user["user_id"])
    return {"access_token": access_token, "token_type": "bearer"}


async def get_current_user(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")

    token = authorization.split(" ", 1)[1]

    if _auth0_enabled():
        claims = _decode_auth0_access_token(token)
        if not claims:
            raise HTTPException(status_code=401, detail="Unauthorized")

        user = await _get_or_create_auth0_user(claims)
        if not user:
            raise HTTPException(status_code=401, detail="Unauthorized")

        return user

    if not _legacy_auth_enabled():
        raise HTTPException(status_code=503, detail="Authentication misconfigured")

    data = decode_access_token(token)
    user_id = data.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    user = await database.db.users.find_one({"user_id": user_id})
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")

    return user


@router.get("/me")
async def me(user=Depends(get_current_user)):
    if user:
        return {
            "email": user.get("email", ""),
            "name": user.get("name", ""),
            "user_id": user.get("user_id", ""),
            "auth_provider": user.get("auth_provider", "email"),
        }
    raise HTTPException(status_code=401)


@router.post("/logout")
async def logout():
    return {"message": "logged out"}
