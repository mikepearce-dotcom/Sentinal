import os
import uuid
import jwt
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from .. import database
from ..models import Token, UserCreate
from ..utils import hash_password, verify_password

router = APIRouter()

# Legacy JWT utilities (kept for local/dev compatibility when Auth0 vars are absent)
JWT_SECRET = os.getenv("JWT_SECRET", "change-me")
JWT_ALGORITHM = "HS256"
TOKEN_EXPIRE_MINUTES = 60

def _clean_env(value: Optional[str]) -> str:
    return str(value or "").strip().strip('"').strip("'")


def _normalize_auth0_domain(value: Optional[str]) -> str:
    cleaned = _clean_env(value).rstrip("/")
    lower = cleaned.lower()
    if lower.startswith("https://"):
        cleaned = cleaned[8:]
    elif lower.startswith("http://"):
        cleaned = cleaned[7:]
    return cleaned.strip().lower()


def _normalize_auth0_audience(value: Optional[str]) -> str:
    return _clean_env(value).rstrip("/")


# Auth0 configuration
AUTH0_DOMAIN = _normalize_auth0_domain(os.getenv("AUTH0_DOMAIN"))
AUTH0_AUDIENCE = _normalize_auth0_audience(os.getenv("AUTH0_AUDIENCE"))
AUTH0_ISSUER = f"https://{AUTH0_DOMAIN}/" if AUTH0_DOMAIN else ""
AUTH0_JWKS_URL = f"{AUTH0_ISSUER}.well-known/jwks.json" if AUTH0_ISSUER else ""

if AUTH0_DOMAIN or AUTH0_AUDIENCE:
    print(f"Auth0 config loaded: domain={AUTH0_DOMAIN}, audience={AUTH0_AUDIENCE}")
else:
    print("Auth0 config missing; using legacy JWT mode.")


def _auth0_enabled() -> bool:
    return bool(AUTH0_DOMAIN and AUTH0_AUDIENCE and AUTH0_ISSUER and AUTH0_JWKS_URL)


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

        return jwt.decode(
            token,
            signing_key.key,
            algorithms=["RS256"],
            audience=expected_audience,
            issuer=AUTH0_ISSUER,
        )
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
    name = _derive_name(
        email,
        _first_non_empty(claims.get("name"), claims.get("nickname"), claims.get("given_name")),
    )

    if not email:
        safe_sub = auth0_sub.replace("|", ".").replace(" ", "")
        email = f"{safe_sub}@auth0.local"

    by_email = await database.db.users.find_one({"email": email})
    if by_email:
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
async def signup(user: UserCreate):
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
async def login(data: LoginData):
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


