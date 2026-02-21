import os
import uuid
from datetime import datetime, timedelta
from functools import lru_cache
from typing import Any, Dict, List, Optional
from urllib.parse import quote, urlsplit

import httpx
import jwt
from fastapi import APIRouter, Depends, Header, HTTPException, Request
from pydantic import BaseModel, EmailStr

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
AUTH0_USERINFO_URL = f"{AUTH0_ISSUER}userinfo" if AUTH0_ISSUER else ""

AUTH0_DB_CONNECTION = clean_env(os.getenv("AUTH0_DB_CONNECTION")) or "Username-Password-Authentication"
AUTH0_MGMT_CLIENT_ID = clean_env(os.getenv("AUTH0_MGMT_CLIENT_ID"))
AUTH0_MGMT_CLIENT_SECRET = clean_env(os.getenv("AUTH0_MGMT_CLIENT_SECRET"))
AUTH0_MGMT_AUDIENCE = clean_env(os.getenv("AUTH0_MGMT_AUDIENCE")) or (
    f"https://{AUTH0_DOMAIN}/api/v2/" if AUTH0_DOMAIN else ""
)
AUTH0_MGMT_TOKEN_URL = f"{AUTH0_ISSUER}oauth/token" if AUTH0_ISSUER else ""
AUTH0_MGMT_USERS_URL = f"{AUTH0_ISSUER}api/v2/users" if AUTH0_ISSUER else ""

AUTH_RATE_WINDOW_SECONDS = parse_int_env(os.getenv("AUTH_RATE_WINDOW_SECONDS"), default=600)
AUTH_LOGIN_RATE_LIMIT = parse_int_env(os.getenv("AUTH_LOGIN_RATE_LIMIT"), default=60)
AUTH_SIGNUP_RATE_LIMIT = parse_int_env(os.getenv("AUTH_SIGNUP_RATE_LIMIT"), default=30)
AUTH_PASSWORD_RESET_RATE_LIMIT = parse_int_env(os.getenv("AUTH_PASSWORD_RESET_RATE_LIMIT"), default=20)
ACCOUNT_DELETE_ENABLED = env_truthy(os.getenv("ACCOUNT_DELETE_ENABLED"), default=False)


def _auth0_enabled() -> bool:
    return bool(AUTH0_DOMAIN and AUTH0_AUDIENCE and AUTH0_ISSUER and AUTH0_JWKS_URL)


def _auth0_mgmt_enabled() -> bool:
    return bool(
        _auth0_enabled()
        and AUTH0_MGMT_CLIENT_ID
        and AUTH0_MGMT_CLIENT_SECRET
        and AUTH0_MGMT_AUDIENCE
        and AUTH0_MGMT_TOKEN_URL
        and AUTH0_MGMT_USERS_URL
    )


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


def _is_placeholder_name(value: Any) -> bool:
    normalized = str(value or "").strip().lower()
    return not normalized or normalized in {"auth0 user", "user", "unknown"}


def _is_placeholder_email(value: Any) -> bool:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return True
    return normalized.endswith("@auth0.local")


def _sanitize_profile_name(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""
    return normalized[:80]


def _sanitize_avatar_url(value: Any) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return ""

    parsed = urlsplit(normalized)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        return ""

    return normalized[:600]


def _effective_avatar_url(user: Dict[str, Any]) -> str:
    return _first_non_empty(user.get("avatar_url"), user.get("auth0_picture_url"))


def _auth_provider_label(user: Dict[str, Any]) -> str:
    auth0_sub = _first_non_empty(user.get("auth0_sub"))
    if "|" in auth0_sub:
        return auth0_sub.split("|", 1)[0]
    return str(user.get("auth_provider") or "email")


def _is_database_auth0_user(user: Dict[str, Any]) -> bool:
    if str(user.get("auth_provider") or "").strip().lower() != "auth0":
        return False

    auth0_sub = _first_non_empty(user.get("auth0_sub"))
    return auth0_sub.lower().startswith("auth0|")


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


async def _fetch_auth0_userinfo(access_token: str) -> Dict[str, Any]:
    token = _first_non_empty(access_token)
    if not token or not _auth0_enabled() or not AUTH0_USERINFO_URL:
        return {}

    try:
        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
            resp = await client.get(
                AUTH0_USERINFO_URL,
                headers={"Authorization": f"Bearer {token}"},
            )
    except Exception as exc:
        print(f"Auth0 userinfo request failed: {type(exc).__name__}: {exc}")
        return {}

    if resp.status_code != 200:
        print(f"Auth0 userinfo request failed: HTTP {resp.status_code}")
        return {}

    try:
        payload = resp.json()
    except Exception as exc:
        print(f"Auth0 userinfo decode failed: {type(exc).__name__}: {exc}")
        return {}

    if not isinstance(payload, dict):
        return {}

    return payload


async def _get_or_create_auth0_user(claims: Dict[str, Any], access_token: str = "") -> Optional[Dict[str, Any]]:
    auth0_sub = _first_non_empty(claims.get("sub"))
    if not auth0_sub:
        return None

    existing = await database.db.users.find_one({"auth0_sub": auth0_sub})
    needs_userinfo = (
        not _first_non_empty(claims.get("email"), claims.get("upn"), claims.get("preferred_username"))
        or not _first_non_empty(claims.get("name"), claims.get("nickname"), claims.get("given_name"))
        or not _first_non_empty(claims.get("picture"))
    )
    if existing:
        if _is_placeholder_email(existing.get("email")) or _is_placeholder_name(existing.get("name")):
            needs_userinfo = True
        if not _first_non_empty(existing.get("auth0_picture_url"), existing.get("avatar_url")):
            needs_userinfo = True

    effective_claims: Dict[str, Any] = dict(claims or {})
    if needs_userinfo:
        userinfo = await _fetch_auth0_userinfo(access_token)
        userinfo_sub = _first_non_empty(userinfo.get("sub"))
        if userinfo and userinfo_sub and userinfo_sub != auth0_sub:
            print("Auth0 userinfo ignored: sub mismatch")
            userinfo = {}

        if userinfo:
            for key in (
                "email",
                "email_verified",
                "name",
                "nickname",
                "given_name",
                "family_name",
                "picture",
            ):
                if key == "email_verified":
                    if key not in effective_claims and key in userinfo:
                        effective_claims[key] = userinfo.get(key)
                    continue

                if not _first_non_empty(effective_claims.get(key)):
                    candidate = userinfo.get(key)
                    if candidate not in (None, ""):
                        effective_claims[key] = candidate

    email = _first_non_empty(
        effective_claims.get("email"),
        effective_claims.get("upn"),
        effective_claims.get("preferred_username"),
    ).lower()
    email_verified_raw = effective_claims.get("email_verified")
    email_verified = email_verified_raw is True or str(email_verified_raw).strip().lower() in {
        "1",
        "true",
        "yes",
    }
    resolved_name = _derive_name(
        email,
        _first_non_empty(
            effective_claims.get("name"),
            effective_claims.get("nickname"),
            effective_claims.get("given_name"),
        ),
    )
    auth0_picture_url = _sanitize_avatar_url(_first_non_empty(effective_claims.get("picture")))

    if existing:
        updates: Dict[str, Any] = {}
        existing_email = str(existing.get("email") or "").strip().lower()
        existing_name = str(existing.get("name") or "").strip()

        if email and email_verified and (_is_placeholder_email(existing_email) or not existing_email):
            updates["email"] = email

        if resolved_name and _is_placeholder_name(existing_name):
            updates["name"] = resolved_name

        if auth0_picture_url and auth0_picture_url != str(existing.get("auth0_picture_url") or ""):
            updates["auth0_picture_url"] = auth0_picture_url

        if str(existing.get("auth_provider") or "").strip().lower() != "auth0":
            updates["auth_provider"] = "auth0"

        if updates:
            await database.db.users.update_one({"_id": existing["_id"]}, {"$set": updates})
            refreshed = await database.db.users.find_one({"_id": existing["_id"]})
            return refreshed or existing

        return existing

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

            updates = {
                "auth0_sub": auth0_sub,
                "auth_provider": "auth0",
            }
            if _is_placeholder_name(by_email.get("name")):
                updates["name"] = resolved_name
            if auth0_picture_url:
                updates["auth0_picture_url"] = auth0_picture_url

            await database.db.users.update_one({"_id": by_email["_id"]}, {"$set": updates})
            refreshed = await database.db.users.find_one({"_id": by_email["_id"]})
            return refreshed or by_email

    # Do not trust unverified email claims as canonical identity.
    if email and not email_verified:
        email = ""

    if not email:
        safe_sub = auth0_sub.replace("|", ".").replace(" ", "")
        email = f"{safe_sub}@auth0.local"

    user_doc = {
        "user_id": str(uuid.uuid4()),
        "email": email,
        "name": resolved_name,
        "auth_provider": "auth0",
        "auth0_sub": auth0_sub,
        "auth0_picture_url": auth0_picture_url,
        "created_at": datetime.utcnow(),
    }
    await database.db.users.insert_one(user_doc)
    return user_doc


async def _send_auth0_password_reset_email(email: str) -> None:
    if not _auth0_enabled() or not AUTH0_CLIENT_ID:
        raise HTTPException(status_code=503, detail="Auth0 password reset is not configured")

    payload = {
        "client_id": AUTH0_CLIENT_ID,
        "email": str(email or "").strip().lower(),
        "connection": AUTH0_DB_CONNECTION,
    }

    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        resp = await client.post(
            f"{AUTH0_ISSUER}dbconnections/change_password",
            json=payload,
            headers={"Content-Type": "application/json"},
        )

    if resp.status_code in (200, 201):
        return

    detail = (resp.text or "").strip()
    if resp.status_code == 400:
        lowered = detail.lower()
        # Avoid user enumeration; treat unknown users as success response.
        if "user does not exist" in lowered or "no user" in lowered:
            return

    raise HTTPException(
        status_code=502,
        detail=f"Auth0 password reset request failed (HTTP {resp.status_code})",
    )


async def _get_auth0_management_token() -> str:
    if not _auth0_mgmt_enabled():
        raise RuntimeError("Auth0 management credentials are not configured")

    payload = {
        "client_id": AUTH0_MGMT_CLIENT_ID,
        "client_secret": AUTH0_MGMT_CLIENT_SECRET,
        "audience": AUTH0_MGMT_AUDIENCE,
        "grant_type": "client_credentials",
    }

    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        resp = await client.post(
            AUTH0_MGMT_TOKEN_URL,
            json=payload,
            headers={"Content-Type": "application/json"},
        )

    if resp.status_code != 200:
        raise RuntimeError(f"Auth0 management token request failed (HTTP {resp.status_code})")

    data = resp.json() if resp.content else {}
    token = str(data.get("access_token") or "").strip()
    if not token:
        raise RuntimeError("Auth0 management token response missing access_token")

    return token


async def _delete_auth0_identity(auth0_sub: str) -> bool:
    normalized = _first_non_empty(auth0_sub)
    if not normalized or not _auth0_mgmt_enabled():
        return False

    try:
        token = await _get_auth0_management_token()
        encoded_sub = quote(normalized, safe="")

        async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
            resp = await client.delete(
                f"{AUTH0_MGMT_USERS_URL}/{encoded_sub}",
                headers={"Authorization": f"Bearer {token}"},
            )

        if resp.status_code in (200, 204, 404):
            return True

        print(f"Auth0 identity delete failed for {normalized}: HTTP {resp.status_code} {resp.text[:200]}")
        return False
    except Exception as exc:
        print(f"Auth0 identity delete error for {normalized}: {exc}")
        return False


class LoginData(BaseModel):
    email: str
    password: str


class PasswordResetRequest(BaseModel):
    email: EmailStr


class AccountProfileUpdate(BaseModel):
    name: Optional[str] = None
    avatar_url: Optional[str] = None


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

        user = await _get_or_create_auth0_user(claims, access_token=token)
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


@router.post("/password-reset-request")
async def password_reset_request(payload: PasswordResetRequest, request: Request):
    _ensure_auth_rate_limit("password_reset_request", request, AUTH_PASSWORD_RESET_RATE_LIMIT)

    await _send_auth0_password_reset_email(str(payload.email or "").strip().lower())

    return {"message": "If an account exists, a password reset email has been sent."}


@router.post("/password-reset")
async def password_reset_current_user(request: Request, user=Depends(get_current_user)):
    _ensure_auth_rate_limit("password_reset_current_user", request, AUTH_PASSWORD_RESET_RATE_LIMIT)

    if not _is_database_auth0_user(user):
        raise HTTPException(
            status_code=400,
            detail="Password reset is available only for email/password Auth0 accounts.",
        )

    email = str(user.get("email") or "").strip().lower()
    if not email:
        raise HTTPException(status_code=400, detail="Current account does not have a resettable email")

    await _send_auth0_password_reset_email(email)

    return {"message": "Password reset email sent."}


@router.get("/me")
async def me(user=Depends(get_current_user)):
    if user:
        return {
            "email": user.get("email", ""),
            "name": user.get("name", ""),
            "user_id": user.get("user_id", ""),
            "auth_provider": user.get("auth_provider", "email"),
            "avatar_url": _effective_avatar_url(user),
        }
    raise HTTPException(status_code=401)


def _build_account_payload(user: Dict[str, Any]) -> Dict[str, Any]:
    provider = _auth_provider_label(user)
    can_reset_password = _is_database_auth0_user(user)
    return {
        "email": user.get("email", ""),
        "name": user.get("name", ""),
        "user_id": user.get("user_id", ""),
        "auth_provider": user.get("auth_provider", "email"),
        "provider": provider,
        "auth0_sub": user.get("auth0_sub", ""),
        "avatar_url": _effective_avatar_url(user),
        "custom_avatar_url": _first_non_empty(user.get("avatar_url")),
        "auth0_picture_url": _first_non_empty(user.get("auth0_picture_url")),
        "can_reset_password": can_reset_password,
        "management_delete_configured": _auth0_mgmt_enabled(),
        "account_delete_enabled": ACCOUNT_DELETE_ENABLED,
    }


@router.get("/account")
async def account(user=Depends(get_current_user)):
    return _build_account_payload(user)


@router.patch("/account/profile")
async def update_account_profile(payload: AccountProfileUpdate, user=Depends(get_current_user)):
    user_id = str(user.get("user_id") or "").strip()
    if not user_id:
        raise HTTPException(status_code=400, detail="Invalid account state")

    updates: Dict[str, Any] = {}

    if payload.name is not None:
        normalized_name = _sanitize_profile_name(payload.name)
        if not normalized_name:
            raise HTTPException(status_code=400, detail="Name cannot be empty")
        updates["name"] = normalized_name

    if payload.avatar_url is not None:
        raw_avatar = str(payload.avatar_url or "").strip()
        normalized_avatar_url = _sanitize_avatar_url(raw_avatar)
        if raw_avatar and not normalized_avatar_url:
            raise HTTPException(status_code=400, detail="Avatar URL must be a valid http(s) URL")
        updates["avatar_url"] = normalized_avatar_url

    if not updates:
        raise HTTPException(status_code=400, detail="No profile changes provided")

    await database.db.users.update_one({"user_id": user_id}, {"$set": updates})
    refreshed = await database.db.users.find_one({"user_id": user_id})
    if not refreshed:
        raise HTTPException(status_code=404, detail="Account not found")

    return _build_account_payload(refreshed)


@router.delete("/account")
async def delete_account(user=Depends(get_current_user)):
    if not ACCOUNT_DELETE_ENABLED:
        raise HTTPException(status_code=403, detail="Account deletion is currently disabled")

    user_id = str(user.get("user_id") or "").strip()
    auth0_sub = _first_non_empty(user.get("auth0_sub"))

    if not user_id:
        raise HTTPException(status_code=400, detail="Invalid account state")

    owned_game_ids: List[str] = []
    cursor = database.db.tracked_games.find({"user_id": user_id}, {"_id": 1})
    async for game in cursor:
        game_id = str(game.get("_id") or "").strip()
        if game_id:
            owned_game_ids.append(game_id)

    if owned_game_ids:
        await database.db.scan_results.delete_many({"game_id": {"$in": owned_game_ids}})

    await database.db.scan_results.delete_many({"user_id": user_id})
    await database.db.tracked_games.delete_many({"user_id": user_id})
    await database.db.users.delete_one({"user_id": user_id})

    auth0_identity_deleted = False
    if auth0_sub:
        auth0_identity_deleted = await _delete_auth0_identity(auth0_sub)

    return {
        "message": "Account data deleted",
        "auth0_identity_deleted": auth0_identity_deleted,
        "requires_auth0_manual_delete": bool(auth0_sub and not auth0_identity_deleted),
    }


@router.post("/logout")
async def logout():
    return {"message": "logged out"}
