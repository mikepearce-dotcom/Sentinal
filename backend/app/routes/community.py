import asyncio
from datetime import datetime, timedelta
import os
import re
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple

import httpx
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel, Field

from .. import database
from ..security import allow_request, client_ip, parse_int_env
from .auth import get_current_user

router = APIRouter()

COMMUNITY_CREATE_RATE_LIMIT = parse_int_env(os.getenv("COMMUNITY_CREATE_RATE_LIMIT"), default=15)
COMMUNITY_SUPPORT_RATE_LIMIT = parse_int_env(os.getenv("COMMUNITY_SUPPORT_RATE_LIMIT"), default=120)
COMMUNITY_WINDOW_SECONDS = parse_int_env(os.getenv("COMMUNITY_RATE_WINDOW_SECONDS"), default=300)

IGDB_TWITCH_CLIENT_ID = str(os.getenv("IGDB_TWITCH_CLIENT_ID") or "").strip()
IGDB_TWITCH_CLIENT_SECRET = str(os.getenv("IGDB_TWITCH_CLIENT_SECRET") or "").strip()
IGDB_REQUEST_TIMEOUT_SECONDS = float(max(5, parse_int_env(os.getenv("IGDB_REQUEST_TIMEOUT_SECONDS"), default=15)))
IGDB_LOGO_CACHE_TTL_SECONDS = max(300, parse_int_env(os.getenv("IGDB_LOGO_CACHE_TTL_SECONDS"), default=7 * 24 * 60 * 60))
IGDB_LOGO_RETRY_MISSING_SECONDS = max(300, parse_int_env(os.getenv("IGDB_LOGO_RETRY_MISSING_SECONDS"), default=24 * 60 * 60))
IGDB_LOGO_IMAGE_SIZE = str(os.getenv("IGDB_LOGO_IMAGE_SIZE") or "cover_small").strip() or "cover_small"

_igdb_token_cache: Dict[str, Any] = {"access_token": "", "expires_at": 0.0}
_igdb_logo_search_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}
_igdb_game_suggest_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
_igdb_token_lock = asyncio.Lock()
IGDB_SUGGEST_CACHE_TTL_SECONDS = 10 * 60

DEFAULT_PETITION_MILESTONES = [100, 500, 1000, 5000]
PETITION_CATEGORIES = [
    "gameplay",
    "content",
    "balance",
    "performance",
    "quality_of_life",
    "accessibility",
    "progression",
    "community",
    "monetization",
    "communication",
    "other",
]
PETITION_CHANGE_TYPES = [
    "feature_request",
    "improvement",
    "balance_change",
    "bug_fix",
    "content_request",
    "policy_change",
    "communication_request",
    "other",
]


class CommunityMetaOut(BaseModel):
    categories: List[str] = Field(default_factory=list)
    change_types: List[str] = Field(default_factory=list)
    default_milestones: List[int] = Field(default_factory=list)


class CommunityGameOut(BaseModel):
    id: str = ""
    slug: str = ""
    name: str
    petition_count: int = 0
    logo_url: str = ""
    source: str = "catalog"


class CommunityGameCreateIn(BaseModel):
    name: str = Field(..., min_length=2, max_length=120)


class CommunityPetitionCreateIn(BaseModel):
    game_id: Optional[str] = ""
    game_name: Optional[str] = ""
    title: str = Field(..., min_length=8, max_length=160)
    summary: str = Field(..., min_length=12, max_length=300)
    body: str = Field(..., min_length=30, max_length=8000)
    category: str = Field(..., min_length=2, max_length=40)
    change_type: str = Field(..., min_length=2, max_length=40)


class CommunityPetitionOut(BaseModel):
    id: str
    slug: str
    title: str
    summary: str = ""
    game_id: str = ""
    game_name: str = ""
    game_logo_url: str = ""
    category: str = "other"
    change_type: str = "other"
    status: str = "published"
    supporter_count: int = 0
    next_milestone: Optional[int] = None
    current_milestone: int = 0
    milestone_progress_pct: float = 0.0
    eligible_for_studio_push: bool = False
    created_at: datetime
    updated_at: datetime
    created_by_name: str = ""
    created_by_avatar_url: str = ""


class CommunityPetitionDetailOut(CommunityPetitionOut):
    body: str = ""
    milestone_targets: List[int] = Field(default_factory=list)
    last_milestone_reached_at: Optional[datetime] = None
    recent_supporters_7d: int = 0
    user_has_supported: bool = False
    top_supporter_goal: Optional[int] = None


class CommunityPetitionListOut(BaseModel):
    items: List[CommunityPetitionOut] = Field(default_factory=list)
    total: int = 0
    page: int = 1
    limit: int = 20


class CommunitySupportStatusOut(BaseModel):
    petition_id: str
    user_has_supported: bool = False
    supporter_count: int = 0
    next_milestone: Optional[int] = None
    current_milestone: int = 0
    milestone_progress_pct: float = 0.0
    eligible_for_studio_push: bool = False


class CommunitySupportActionOut(CommunitySupportStatusOut):
    action: str = "supported"


class CommunityMyPetitionsOut(BaseModel):
    items: List[CommunityPetitionOut] = Field(default_factory=list)


class CommunityMilestoneCandidatesOut(BaseModel):
    items: List[CommunityPetitionOut] = Field(default_factory=list)


def _safe_str(value: Any, max_len: int = 0) -> str:
    text = str(value or "").strip()
    if max_len > 0:
        return text[:max_len]
    return text


def _user_avatar(user: Dict[str, Any]) -> str:
    for key in ("avatar_url", "auth0_picture_url"):
        value = _safe_str(user.get(key), 600)
        if value:
            return value
    return ""


def _normalize_key(value: str) -> str:
    return " ".join(re.findall(r"[a-z0-9]+", _safe_str(value).lower()))


def _slugify(value: str, fallback: str = "item") -> str:
    base = re.sub(r"[^a-z0-9]+", "-", _safe_str(value).lower()).strip("-")
    base = re.sub(r"-{2,}", "-", base)
    return (base or fallback)[:90].strip("-") or fallback


def _igdb_enabled() -> bool:
    return bool(IGDB_TWITCH_CLIENT_ID and IGDB_TWITCH_CLIENT_SECRET)


def _igdb_cache_key(game_name: str) -> str:
    return _normalize_key(game_name)


def _igdb_logo_cache_get(cache_key: str) -> Optional[Dict[str, Any]]:
    if not cache_key:
        return None
    cached = _igdb_logo_search_cache.get(cache_key)
    if not cached:
        return None
    expires_at, payload = cached
    if float(expires_at or 0.0) <= time.time():
        _igdb_logo_search_cache.pop(cache_key, None)
        return None
    return dict(payload or {})


def _igdb_logo_cache_set(cache_key: str, payload: Dict[str, Any]) -> None:
    if not cache_key:
        return
    safe_payload = dict(payload or {})
    ttl = IGDB_LOGO_CACHE_TTL_SECONDS if safe_payload.get("logo_url") else IGDB_LOGO_RETRY_MISSING_SECONDS
    _igdb_logo_search_cache[cache_key] = (time.time() + float(ttl), safe_payload)


def _igdb_image_url(image_id: str, size: str = "") -> str:
    clean_image_id = _safe_str(image_id, 80)
    if not clean_image_id:
        return ""
    size_name = _safe_str(size or IGDB_LOGO_IMAGE_SIZE, 40) or "cover_small"
    if size_name.startswith("t_"):
        size_name = size_name[2:]
    return f"https://images.igdb.com/igdb/image/upload/t_{size_name}/{clean_image_id}.jpg"


def _igdb_candidate_score(game_name: str, candidate: Dict[str, Any]) -> float:
    query = _normalize_key(game_name)
    candidate_name = _safe_str(candidate.get("name"), 140)
    candidate_norm = _normalize_key(candidate_name)
    if not query or not candidate_norm:
        return -1.0

    score = 0.0
    if candidate_norm == query:
        score += 100.0
    if candidate_norm.startswith(query):
        score += 25.0
    if query.startswith(candidate_norm):
        score += 18.0
    if query in candidate_norm and candidate_norm != query:
        score += 20.0
    if candidate_norm in query and candidate_norm != query:
        score += 12.0

    query_tokens = set(query.split())
    candidate_tokens = set(candidate_norm.split())
    if query_tokens and candidate_tokens:
        overlap = len(query_tokens.intersection(candidate_tokens))
        score += (overlap / float(len(query_tokens))) * 30.0
        score += (overlap / float(len(candidate_tokens))) * 10.0

    cover = candidate.get("cover") if isinstance(candidate.get("cover"), dict) else {}
    if _safe_str(cover.get("image_id")):
        score += 8.0

    try:
        if int(candidate.get("first_release_date") or 0) > 0:
            score += 1.0
    except Exception:
        pass

    return score


async def _igdb_get_access_token() -> str:
    if not _igdb_enabled():
        return ""

    cached_token = _safe_str(_igdb_token_cache.get("access_token"), 2000)
    cached_expires_at = float(_igdb_token_cache.get("expires_at") or 0.0)
    if cached_token and cached_expires_at > (time.time() + 60.0):
        return cached_token

    async with _igdb_token_lock:
        cached_token = _safe_str(_igdb_token_cache.get("access_token"), 2000)
        cached_expires_at = float(_igdb_token_cache.get("expires_at") or 0.0)
        if cached_token and cached_expires_at > (time.time() + 60.0):
            return cached_token

        try:
            async with httpx.AsyncClient(timeout=IGDB_REQUEST_TIMEOUT_SECONDS, follow_redirects=True) as client:
                resp = await client.post(
                    "https://id.twitch.tv/oauth2/token",
                    params={
                        "client_id": IGDB_TWITCH_CLIENT_ID,
                        "client_secret": IGDB_TWITCH_CLIENT_SECRET,
                        "grant_type": "client_credentials",
                    },
                    headers={"Accept": "application/json"},
                )
        except Exception as exc:
            print(f"IGDB token request failed: {exc}")
            return ""

        if resp.status_code != 200:
            excerpt = _safe_str(resp.text, 240)
            print(f"IGDB token request failed (HTTP {resp.status_code}): {excerpt}")
            return ""

        try:
            payload = resp.json()
        except Exception:
            print("IGDB token response was not valid JSON")
            return ""

        access_token = _safe_str((payload or {}).get("access_token"), 4000)
        expires_in = int((payload or {}).get("expires_in") or 0)
        if not access_token:
            return ""

        _igdb_token_cache["access_token"] = access_token
        _igdb_token_cache["expires_at"] = time.time() + max(60, expires_in - 60)
        return access_token


async def _igdb_search_game_logo(game_name: str) -> Dict[str, Any]:
    clean_name = _safe_str(game_name, 120)
    cache_key = _igdb_cache_key(clean_name)
    if not clean_name or not cache_key:
        return {}

    cached = _igdb_logo_cache_get(cache_key)
    if cached is not None:
        return cached

    if not _igdb_enabled():
        return {}

    token = await _igdb_get_access_token()
    if not token:
        return {}

    escaped = clean_name.replace('\\', '\\\\').replace('"', '\\"')
    query = (
        f'search "{escaped}"; '
        'fields id,name,slug,first_release_date,cover.image_id; '
        'limit 8;'
    )

    try:
        async with httpx.AsyncClient(timeout=IGDB_REQUEST_TIMEOUT_SECONDS, follow_redirects=True) as client:
            resp = await client.post(
                "https://api.igdb.com/v4/games",
                content=query,
                headers={
                    "Client-ID": IGDB_TWITCH_CLIENT_ID,
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                    "Content-Type": "text/plain",
                },
            )
    except Exception as exc:
        print(f"IGDB game search failed ({clean_name}): {exc}")
        return {}

    if resp.status_code != 200:
        excerpt = _safe_str(resp.text, 240)
        print(f"IGDB game search failed ({clean_name}) HTTP {resp.status_code}: {excerpt}")
        return {}

    try:
        rows = resp.json()
    except Exception:
        print(f"IGDB game search invalid JSON ({clean_name})")
        return {}

    if not isinstance(rows, list):
        _igdb_logo_cache_set(cache_key, {})
        return {}

    best: Optional[Dict[str, Any]] = None
    best_score = -1.0
    for row in rows:
        if not isinstance(row, dict):
            continue
        score = _igdb_candidate_score(clean_name, row)
        cover = row.get("cover") if isinstance(row.get("cover"), dict) else {}
        if not _safe_str(cover.get("image_id")):
            score -= 6.0
        if score > best_score:
            best = row
            best_score = score

    if not best or best_score < 20.0:
        _igdb_logo_cache_set(cache_key, {})
        return {}

    cover = best.get("cover") if isinstance(best.get("cover"), dict) else {}
    logo_url = _igdb_image_url(_safe_str(cover.get("image_id")))
    if not logo_url:
        _igdb_logo_cache_set(cache_key, {})
        return {}

    payload = {
        "logo_url": logo_url,
        "logo_source": "igdb",
        "igdb_game_id": str(best.get("id") or ""),
        "igdb_slug": _safe_str(best.get("slug"), 120),
        "igdb_name": _safe_str(best.get("name"), 140),
    }
    _igdb_logo_cache_set(cache_key, payload)
    return payload


def _igdb_suggest_cache_get(query_text: str) -> Optional[List[Dict[str, Any]]]:
    cache_key = _normalize_key(query_text)
    if not cache_key:
        return None
    cached = _igdb_game_suggest_cache.get(cache_key)
    if not cached:
        return None
    expires_at, rows = cached
    if float(expires_at or 0.0) <= time.time():
        _igdb_game_suggest_cache.pop(cache_key, None)
        return None
    return [dict(item) for item in (rows or []) if isinstance(item, dict)]


def _igdb_suggest_cache_set(query_text: str, rows: List[Dict[str, Any]]) -> None:
    cache_key = _normalize_key(query_text)
    if not cache_key:
        return
    safe_rows = [dict(item) for item in rows if isinstance(item, dict)]
    _igdb_game_suggest_cache[cache_key] = (time.time() + IGDB_SUGGEST_CACHE_TTL_SECONDS, safe_rows)


async def _igdb_search_game_suggestions(query_text: str, limit: int = 8) -> List[Dict[str, Any]]:
    clean_query = _safe_str(query_text, 120)
    safe_limit = max(1, min(int(limit or 8), 20))
    if len(clean_query) < 2 or not _igdb_enabled():
        return []

    cached = _igdb_suggest_cache_get(clean_query)
    if cached is not None:
        return cached[:safe_limit]

    token = await _igdb_get_access_token()
    if not token:
        return []

    escaped = clean_query.replace('\\', '\\\\').replace('"', '\\"')
    query = (
        f'search "{escaped}"; '
        'fields id,name,slug,first_release_date,cover.image_id; '
        f'limit {max(8, safe_limit * 2)};'
    )

    try:
        async with httpx.AsyncClient(timeout=IGDB_REQUEST_TIMEOUT_SECONDS, follow_redirects=True) as client:
            resp = await client.post(
                "https://api.igdb.com/v4/games",
                content=query,
                headers={
                    "Client-ID": IGDB_TWITCH_CLIENT_ID,
                    "Authorization": f"Bearer {token}",
                    "Accept": "application/json",
                    "Content-Type": "text/plain",
                },
            )
    except Exception as exc:
        print(f"IGDB game suggestions failed ({clean_query}): {exc}")
        return []

    if resp.status_code != 200:
        excerpt = _safe_str(resp.text, 240)
        print(f"IGDB game suggestions failed ({clean_query}) HTTP {resp.status_code}: {excerpt}")
        return []

    try:
        raw_rows = resp.json()
    except Exception:
        print(f"IGDB game suggestions invalid JSON ({clean_query})")
        return []

    if not isinstance(raw_rows, list):
        _igdb_suggest_cache_set(clean_query, [])
        return []

    scored: List[Tuple[float, Dict[str, Any]]] = []
    seen = set()
    for row in raw_rows:
        if not isinstance(row, dict):
            continue
        name = _safe_str(row.get("name"), 140)
        key = _normalize_key(name)
        if not name or not key or key in seen:
            continue

        score = _igdb_candidate_score(clean_query, row)
        cover = row.get("cover") if isinstance(row.get("cover"), dict) else {}
        image_id = _safe_str(cover.get("image_id"), 80)
        if image_id:
            score += 4.0
        if score < 10.0:
            continue

        seen.add(key)
        scored.append(
            (
                score,
                {
                    "id": "",
                    "slug": _slugify(name),
                    "name": name,
                    "petition_count": 0,
                    "logo_url": _igdb_image_url(image_id) if image_id else "",
                    "source": "igdb",
                    "igdb_game_id": _safe_str(row.get("id"), 40),
                },
            )
        )

    scored.sort(key=lambda item: item[0], reverse=True)
    rows = [item for _, item in scored[:safe_limit]]
    _igdb_suggest_cache_set(clean_query, rows)
    return rows


async def _ensure_community_game_logo(game_doc: Dict[str, Any], persist: bool = True) -> Dict[str, Any]:
    if not isinstance(game_doc, dict):
        return game_doc

    if _safe_str(game_doc.get("logo_url"), 600):
        return game_doc
    if not _igdb_enabled():
        return game_doc

    game_name = _safe_str(game_doc.get("name"), 120)
    game_id = _safe_str(game_doc.get("_id") or game_doc.get("id"), 64)
    if not game_name:
        return game_doc

    status = _safe_str(game_doc.get("logo_status"), 20).lower()
    last_verified = game_doc.get("logo_last_verified_at")
    if status == "missing" and isinstance(last_verified, datetime):
        age_seconds = (datetime.utcnow() - last_verified).total_seconds()
        if age_seconds < float(IGDB_LOGO_RETRY_MISSING_SECONDS):
            return game_doc

    resolved = await _igdb_search_game_logo(game_name)
    now = datetime.utcnow()

    if resolved.get("logo_url"):
        updates: Dict[str, Any] = {
            "logo_url": _safe_str(resolved.get("logo_url"), 600),
            "logo_source": _safe_str(resolved.get("logo_source"), 40) or "igdb",
            "logo_status": "resolved",
            "logo_last_verified_at": now,
            "updated_at": now,
        }
        if _safe_str(resolved.get("igdb_game_id"), 40):
            updates["igdb_game_id"] = _safe_str(resolved.get("igdb_game_id"), 40)
        if _safe_str(resolved.get("igdb_slug"), 120):
            updates["igdb_slug"] = _safe_str(resolved.get("igdb_slug"), 120)
        if _safe_str(resolved.get("igdb_name"), 140):
            updates["igdb_name"] = _safe_str(resolved.get("igdb_name"), 140)

        merged = dict(game_doc)
        merged.update(updates)
        if persist and game_id:
            await database.db.community_games.update_one({"_id": game_id}, {"$set": updates})
        return merged

    missing_updates = {
        "logo_status": "missing",
        "logo_last_verified_at": now,
        "updated_at": now,
    }
    merged = dict(game_doc)
    merged.update(missing_updates)
    if persist and game_id:
        await database.db.community_games.update_one({"_id": game_id}, {"$set": missing_updates})
    return merged


async def _hydrate_petition_game_logos(docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows = [doc for doc in docs if isinstance(doc, dict)]
    if not rows:
        return rows

    missing_game_ids: List[str] = []
    for row in rows:
        if _safe_str(row.get("game_logo_url"), 600):
            continue
        game_id = _safe_str(row.get("game_id"), 64)
        if game_id and game_id not in missing_game_ids:
            missing_game_ids.append(game_id)

    if not missing_game_ids:
        return rows

    game_map: Dict[str, Dict[str, Any]] = {}
    async for game in database.db.community_games.find({"_id": {"$in": missing_game_ids}}):
        game_doc = dict(game)
        if not _safe_str(game_doc.get("logo_url"), 600):
            game_doc = await _ensure_community_game_logo(game_doc, persist=True)
        game_map[_safe_str(game_doc.get("_id"), 64)] = game_doc

    for row in rows:
        if _safe_str(row.get("game_logo_url"), 600):
            continue
        game_id = _safe_str(row.get("game_id"), 64)
        logo_url = _safe_str((game_map.get(game_id) or {}).get("logo_url"), 600)
        if logo_url:
            row["game_logo_url"] = logo_url

    return rows


async def _unique_slug(collection_name: str, base_slug: str, field: str = "slug") -> str:
    candidate = _slugify(base_slug)
    if not candidate:
        candidate = "item"

    for idx in range(1, 2000):
        attempt = candidate if idx == 1 else f"{candidate}-{idx}"
        existing = await database.db[collection_name].find_one({field: attempt}, {"_id": 1})
        if not existing:
            return attempt

    return f"{candidate}-{uuid.uuid4().hex[:8]}"


def _ensure_rate_limit(scope: str, request: Request, limit: int) -> None:
    key = f"community:{scope}:{client_ip(request)}"
    if allow_request(key, limit=limit, window_seconds=COMMUNITY_WINDOW_SECONDS):
        return
    raise HTTPException(status_code=429, detail="Too many requests. Please slow down and try again.")


def _canonical_category(value: str) -> str:
    normalized = _slugify(value, fallback="other").replace("-", "_")
    return normalized if normalized in PETITION_CATEGORIES else "other"


def _canonical_change_type(value: str) -> str:
    normalized = _slugify(value, fallback="other").replace("-", "_")
    return normalized if normalized in PETITION_CHANGE_TYPES else "other"


def _milestone_summary(count: int, milestones: Optional[List[int]] = None) -> Dict[str, Any]:
    targets = [int(v) for v in (milestones or DEFAULT_PETITION_MILESTONES) if int(v) > 0]
    targets = sorted(list(dict.fromkeys(targets))) or list(DEFAULT_PETITION_MILESTONES)

    current = 0
    next_target: Optional[int] = None
    for target in targets:
        if count >= target:
            current = target
            continue
        next_target = target
        break

    if next_target:
        progress = round(min(100.0, (float(count) / float(next_target)) * 100.0), 1) if next_target > 0 else 0.0
    else:
        progress = 100.0

    return {
        "milestones": targets,
        "current_milestone": current,
        "next_milestone": next_target,
        "milestone_progress_pct": progress,
        "eligible_for_studio_push": bool(current >= targets[0]),
        "top_supporter_goal": targets[-1] if targets else None,
    }


def _petition_out_from_doc(doc: Dict[str, Any]) -> CommunityPetitionOut:
    supporter_count = int(doc.get("supporter_count", 0) or 0)
    milestone = _milestone_summary(supporter_count, doc.get("milestone_targets") if isinstance(doc.get("milestone_targets"), list) else None)
    return CommunityPetitionOut(
        id=_safe_str(doc.get("_id") or doc.get("id")),
        slug=_safe_str(doc.get("slug")),
        title=_safe_str(doc.get("title"), 160),
        summary=_safe_str(doc.get("summary"), 300),
        game_id=_safe_str(doc.get("game_id")),
        game_name=_safe_str(doc.get("game_name"), 120),
        game_logo_url=_safe_str(doc.get("game_logo_url"), 600),
        category=_safe_str(doc.get("category")) or "other",
        change_type=_safe_str(doc.get("change_type")) or "other",
        status=_safe_str(doc.get("status")) or "published",
        supporter_count=supporter_count,
        next_milestone=milestone["next_milestone"],
        current_milestone=milestone["current_milestone"],
        milestone_progress_pct=float(milestone["milestone_progress_pct"]),
        eligible_for_studio_push=bool(milestone["eligible_for_studio_push"]),
        created_at=doc.get("created_at"),
        updated_at=doc.get("updated_at") or doc.get("created_at"),
        created_by_name=_safe_str(doc.get("created_by_name"), 80),
        created_by_avatar_url=_safe_str(doc.get("created_by_avatar_url"), 600),
    )


def _petition_detail_out_from_doc(doc: Dict[str, Any], user_has_supported: bool = False) -> CommunityPetitionDetailOut:
    base = _petition_out_from_doc(doc)
    milestone = _milestone_summary(base.supporter_count, doc.get("milestone_targets") if isinstance(doc.get("milestone_targets"), list) else None)
    return CommunityPetitionDetailOut(
        **base.dict(),
        body=_safe_str(doc.get("body"), 8000),
        milestone_targets=milestone["milestones"],
        last_milestone_reached_at=doc.get("last_milestone_reached_at"),
        recent_supporters_7d=int(doc.get("recent_supporters_7d", 0) or 0),
        user_has_supported=bool(user_has_supported),
        top_supporter_goal=milestone["top_supporter_goal"],
    )


def _build_petition_search_query(
    *,
    game_id: str = "",
    category: str = "",
    status: str = "published",
    q: str = "",
) -> Dict[str, Any]:
    query: Dict[str, Any] = {}
    status_value = _safe_str(status).lower()
    if status_value:
        query["status"] = status_value
    if _safe_str(game_id):
        query["game_id"] = _safe_str(game_id)
    if _safe_str(category):
        query["category"] = _canonical_category(category)
    if _safe_str(q):
        pattern = re.escape(_safe_str(q))
        query["$or"] = [
            {"title": {"$regex": pattern, "$options": "i"}},
            {"summary": {"$regex": pattern, "$options": "i"}},
            {"body": {"$regex": pattern, "$options": "i"}},
            {"game_name": {"$regex": pattern, "$options": "i"}},
        ]
    return query


def _petition_sort(sort: str):
    key = _safe_str(sort).lower()
    if key == "new":
        return [("created_at", -1), ("supporter_count", -1)]
    if key == "top":
        return [("supporter_count", -1), ("created_at", -1)]
    # "momentum" fallback using latest support + supporter count.
    return [("last_support_at", -1), ("supporter_count", -1), ("created_at", -1)]


async def _recompute_petition_support_snapshot(petition_id: str) -> Dict[str, Any]:
    signatures_coll = database.db.community_petition_signatures
    petitions_coll = database.db.community_petitions

    supporter_count = int(await signatures_coll.count_documents({"petition_id": petition_id}))
    seven_days_ago = datetime.utcnow() - timedelta(days=7)
    recent_supporters_7d = int(
        await signatures_coll.count_documents({"petition_id": petition_id, "created_at": {"$gte": seven_days_ago}})
    )

    petition = await petitions_coll.find_one({"_id": petition_id})
    if not petition:
        raise HTTPException(status_code=404, detail="Petition not found")

    milestone = _milestone_summary(supporter_count, petition.get("milestone_targets") if isinstance(petition.get("milestone_targets"), list) else None)
    updates: Dict[str, Any] = {
        "supporter_count": supporter_count,
        "recent_supporters_7d": recent_supporters_7d,
        "updated_at": datetime.utcnow(),
        "current_milestone": milestone["current_milestone"],
        "next_milestone": milestone["next_milestone"],
        "milestone_progress_pct": milestone["milestone_progress_pct"],
        "eligible_for_studio_push": milestone["eligible_for_studio_push"],
    }

    prev_current = int(petition.get("current_milestone", 0) or 0)
    if int(milestone["current_milestone"] or 0) > prev_current:
        updates["last_milestone_reached_at"] = datetime.utcnow()
    if supporter_count > 0:
        updates["last_support_at"] = datetime.utcnow()

    await petitions_coll.update_one({"_id": petition_id}, {"$set": updates})
    refreshed = await petitions_coll.find_one({"_id": petition_id})
    return refreshed or {**petition, **updates}


async def _get_petition_by_ref(petition_ref: str) -> Optional[Dict[str, Any]]:
    ref = _safe_str(petition_ref)
    if not ref:
        return None
    doc = await database.db.community_petitions.find_one({"_id": ref})
    if doc:
        return doc
    return await database.db.community_petitions.find_one({"slug": ref})


async def _ensure_community_game(game_id: str, game_name: str, user: Dict[str, Any]) -> Dict[str, Any]:
    community_games = database.db.community_games
    tracked_games = database.db.tracked_games

    clean_game_id = _safe_str(game_id)
    clean_game_name = _safe_str(game_name, 120)

    if clean_game_id:
        existing = await community_games.find_one({"_id": clean_game_id})
        if not existing:
            raise HTTPException(status_code=404, detail="Selected game not found")
        return await _ensure_community_game_logo(existing, persist=True)

    if not clean_game_name:
        raise HTTPException(status_code=400, detail="Game selection is required")

    normalized_name = _normalize_key(clean_game_name)
    if not normalized_name:
        raise HTTPException(status_code=400, detail="Invalid game name")

    existing = await community_games.find_one({"normalized_name": normalized_name})
    if existing:
        return await _ensure_community_game_logo(existing, persist=True)

    tracked_match = await tracked_games.find_one(
        {"name": {"$regex": f"^{re.escape(clean_game_name)}$", "$options": "i"}},
        {"name": 1, "subreddit": 1},
    )

    canonical_name = _safe_str((tracked_match or {}).get("name") or clean_game_name, 120)
    slug = await _unique_slug("community_games", canonical_name)

    now = datetime.utcnow()
    doc = {
        "_id": str(uuid.uuid4()),
        "slug": slug,
        "name": canonical_name,
        "normalized_name": _normalize_key(canonical_name),
        "subreddit": _safe_str((tracked_match or {}).get("subreddit"), 80),
        "petition_count": 0,
        "logo_url": "",
        "logo_source": "",
        "logo_status": "pending" if _igdb_enabled() else "disabled",
        "logo_last_verified_at": None,
        "created_by_user_id": _safe_str(user.get("user_id")),
        "created_at": now,
        "updated_at": now,
    }
    doc = await _ensure_community_game_logo(doc, persist=False)
    await community_games.insert_one(doc)
    return doc


async def _refresh_game_petition_count(game_id: str) -> None:
    clean_game_id = _safe_str(game_id)
    if not clean_game_id:
        return
    count = int(await database.db.community_petitions.count_documents({"game_id": clean_game_id, "status": "published"}))
    await database.db.community_games.update_one(
        {"_id": clean_game_id},
        {"$set": {"petition_count": count, "updated_at": datetime.utcnow()}},
    )


@router.get("/metadata", response_model=CommunityMetaOut)
async def community_metadata():
    return CommunityMetaOut(
        categories=PETITION_CATEGORIES,
        change_types=PETITION_CHANGE_TYPES,
        default_milestones=DEFAULT_PETITION_MILESTONES,
    )


@router.get("/games/search", response_model=List[CommunityGameOut])
async def search_community_games(
    q: str = Query("", min_length=0, max_length=120),
    limit: int = Query(12, ge=1, le=50),
):
    community_games = database.db.community_games
    tracked_games = database.db.tracked_games
    results: List[CommunityGameOut] = []
    seen_names = set()

    query_text = _safe_str(q, 120)
    if query_text:
        regex = {"$regex": re.escape(query_text), "$options": "i"}
        cursor = community_games.find({"name": regex}).sort("petition_count", -1).limit(limit)
    else:
        cursor = community_games.find({}).sort("petition_count", -1).limit(limit)

    async for row in cursor:
        name = _safe_str(row.get("name"), 120)
        key = _normalize_key(name)
        if not name or not key or key in seen_names:
            continue
        seen_names.add(key)
        results.append(
            CommunityGameOut(
                id=_safe_str(row.get("_id")),
                slug=_safe_str(row.get("slug")),
                name=name,
                petition_count=int(row.get("petition_count", 0) or 0),
                logo_url=_safe_str(row.get("logo_url"), 600),
                source="catalog",
            )
        )

    if len(results) < limit:
        tracked_query = {"name": {"$regex": re.escape(query_text), "$options": "i"}} if query_text else {}
        cursor = tracked_games.find(tracked_query, {"name": 1}).limit(limit * 2)
        async for row in cursor:
            if len(results) >= limit:
                break
            name = _safe_str(row.get("name"), 120)
            key = _normalize_key(name)
            if not name or not key or key in seen_names:
                continue
            seen_names.add(key)
            results.append(CommunityGameOut(id="", slug=_slugify(name), name=name, petition_count=0, logo_url="", source="suggested"))

    if query_text and len(results) < limit and _igdb_enabled():
        try:
            igdb_rows = await _igdb_search_game_suggestions(query_text, limit=(limit - len(results)))
        except Exception as exc:
            print(f"IGDB suggestion lookup failed ({query_text}): {exc}")
            igdb_rows = []

        for row in igdb_rows:
            name = _safe_str(row.get("name"), 120)
            key = _normalize_key(name)
            if not name or not key or key in seen_names:
                continue
            seen_names.add(key)
            results.append(
                CommunityGameOut(
                    id="",
                    slug=_safe_str(row.get("slug"), 90) or _slugify(name),
                    name=name,
                    petition_count=0,
                    logo_url=_safe_str(row.get("logo_url"), 600),
                    source="igdb",
                )
            )
            if len(results) >= limit:
                break

    return results[:limit]


@router.post("/games", response_model=CommunityGameOut)
async def create_community_game(payload: CommunityGameCreateIn, request: Request, user=Depends(get_current_user)):
    _ensure_rate_limit("create_game", request, COMMUNITY_CREATE_RATE_LIMIT)
    game_doc = await _ensure_community_game("", payload.name, user)
    return CommunityGameOut(
        id=_safe_str(game_doc.get("_id")),
        slug=_safe_str(game_doc.get("slug")),
        name=_safe_str(game_doc.get("name"), 120),
        petition_count=int(game_doc.get("petition_count", 0) or 0),
        logo_url=_safe_str(game_doc.get("logo_url"), 600),
        source="catalog",
    )


@router.get("/petitions", response_model=CommunityPetitionListOut)
async def list_petitions(
    q: str = Query("", min_length=0, max_length=120),
    game_id: str = Query("", max_length=64),
    category: str = Query("", max_length=40),
    status: str = Query("published", max_length=40),
    sort: str = Query("momentum", max_length=20),
    page: int = Query(1, ge=1, le=1000),
    limit: int = Query(20, ge=1, le=50),
):
    query = _build_petition_search_query(game_id=game_id, category=category, status=status, q=q)
    total = int(await database.db.community_petitions.count_documents(query))
    skip = (page - 1) * limit

    cursor = (
        database.db.community_petitions
        .find(query)
        .sort(_petition_sort(sort))
        .skip(skip)
        .limit(limit)
    )

    rows: List[Dict[str, Any]] = []
    async for row in cursor:
        rows.append(dict(row))
    rows = await _hydrate_petition_game_logos(rows)
    items = [_petition_out_from_doc(row) for row in rows]

    return CommunityPetitionListOut(items=items, total=total, page=page, limit=limit)


@router.get("/petitions/mine", response_model=CommunityMyPetitionsOut)
async def my_petitions(user=Depends(get_current_user)):
    cursor = (
        database.db.community_petitions
        .find({"created_by_user_id": _safe_str(user.get("user_id"))})
        .sort([("created_at", -1)])
        .limit(100)
    )
    rows: List[Dict[str, Any]] = []
    async for row in cursor:
        rows.append(dict(row))
    rows = await _hydrate_petition_game_logos(rows)
    items = [_petition_out_from_doc(row) for row in rows]
    return CommunityMyPetitionsOut(items=items)


@router.get("/petitions/milestone-candidates", response_model=CommunityMilestoneCandidatesOut)
async def milestone_candidates(
    min_supporters: int = Query(DEFAULT_PETITION_MILESTONES[0], ge=1, le=1000000),
    limit: int = Query(25, ge=1, le=100),
):
    cursor = (
        database.db.community_petitions
        .find({"status": "published", "supporter_count": {"$gte": int(min_supporters)}})
        .sort([("supporter_count", -1), ("last_support_at", -1), ("created_at", -1)])
        .limit(limit)
    )
    rows: List[Dict[str, Any]] = []
    async for row in cursor:
        rows.append(dict(row))
    rows = await _hydrate_petition_game_logos(rows)
    items = [_petition_out_from_doc(row) for row in rows]
    return CommunityMilestoneCandidatesOut(items=items)


@router.post("/petitions", response_model=CommunityPetitionDetailOut)
async def create_petition(payload: CommunityPetitionCreateIn, request: Request, user=Depends(get_current_user)):
    _ensure_rate_limit("create_petition", request, COMMUNITY_CREATE_RATE_LIMIT)

    title = _safe_str(payload.title, 160)
    summary = _safe_str(payload.summary, 300)
    body = _safe_str(payload.body, 8000)
    if len(title) < 8 or len(summary) < 12 or len(body) < 30:
        raise HTTPException(status_code=400, detail="Petition title, summary, and body must be filled out")

    category = _canonical_category(payload.category)
    change_type = _canonical_change_type(payload.change_type)
    game_doc = await _ensure_community_game(_safe_str(payload.game_id), _safe_str(payload.game_name, 120), user)

    normalized_title = _normalize_key(title)
    duplicate = await database.db.community_petitions.find_one(
        {
            "game_id": _safe_str(game_doc.get("_id")),
            "normalized_title": normalized_title,
            "status": {"$in": ["published", "under_review"]},
        },
        {"_id": 1, "slug": 1},
    )
    if duplicate:
        raise HTTPException(status_code=409, detail="A similar petition for this game already exists")

    slug_base = f"{_safe_str(game_doc.get('name'))}-{title}"
    slug = await _unique_slug("community_petitions", slug_base)
    now = datetime.utcnow()

    doc = {
        "_id": str(uuid.uuid4()),
        "slug": slug,
        "title": title,
        "normalized_title": normalized_title,
        "summary": summary,
        "body": body,
        "game_id": _safe_str(game_doc.get("_id")),
        "game_name": _safe_str(game_doc.get("name"), 120),
        "game_logo_url": _safe_str(game_doc.get("logo_url"), 600),
        "category": category,
        "change_type": change_type,
        "status": "published",
        "supporter_count": 0,
        "recent_supporters_7d": 0,
        "milestone_targets": list(DEFAULT_PETITION_MILESTONES),
        "current_milestone": 0,
        "next_milestone": DEFAULT_PETITION_MILESTONES[0],
        "milestone_progress_pct": 0.0,
        "eligible_for_studio_push": False,
        "created_by_user_id": _safe_str(user.get("user_id")),
        "created_by_name": _safe_str(user.get("name"), 80) or "Community User",
        "created_by_avatar_url": _user_avatar(user),
        "created_at": now,
        "updated_at": now,
        "last_support_at": now,
    }

    await database.db.community_petitions.insert_one(doc)
    await _refresh_game_petition_count(_safe_str(game_doc.get("_id")))

    return _petition_detail_out_from_doc(doc, user_has_supported=False)


@router.get("/petitions/{petition_ref}", response_model=CommunityPetitionDetailOut)
async def get_petition(petition_ref: str):
    petition = await _get_petition_by_ref(petition_ref)
    if not petition or _safe_str(petition.get("status"), 40).lower() not in {"published", "under_review", "sent_to_studio", "acknowledged"}:
        raise HTTPException(status_code=404, detail="Petition not found")
    hydrated_rows = await _hydrate_petition_game_logos([dict(petition)])
    hydrated = hydrated_rows[0] if hydrated_rows else dict(petition)
    return _petition_detail_out_from_doc(hydrated, user_has_supported=False)


@router.get("/petitions/{petition_ref}/support-status", response_model=CommunitySupportStatusOut)
async def petition_support_status(petition_ref: str, user=Depends(get_current_user)):
    petition = await _get_petition_by_ref(petition_ref)
    if not petition:
        raise HTTPException(status_code=404, detail="Petition not found")

    petition_id = _safe_str(petition.get("_id"))
    user_id = _safe_str(user.get("user_id"))
    existing = await database.db.community_petition_signatures.find_one({"petition_id": petition_id, "user_id": user_id}, {"_id": 1})
    out = _petition_out_from_doc(petition)
    return CommunitySupportStatusOut(
        petition_id=petition_id,
        user_has_supported=bool(existing),
        supporter_count=out.supporter_count,
        next_milestone=out.next_milestone,
        current_milestone=out.current_milestone,
        milestone_progress_pct=out.milestone_progress_pct,
        eligible_for_studio_push=out.eligible_for_studio_push,
    )


@router.post("/petitions/{petition_ref}/support", response_model=CommunitySupportActionOut)
async def support_petition(petition_ref: str, request: Request, user=Depends(get_current_user)):
    _ensure_rate_limit("support_petition", request, COMMUNITY_SUPPORT_RATE_LIMIT)

    petition = await _get_petition_by_ref(petition_ref)
    if not petition:
        raise HTTPException(status_code=404, detail="Petition not found")
    if _safe_str(petition.get("status"), 40).lower() != "published":
        raise HTTPException(status_code=400, detail="Only published petitions can be supported")

    petition_id = _safe_str(petition.get("_id"))
    user_id = _safe_str(user.get("user_id"))
    signatures = database.db.community_petition_signatures
    existing = await signatures.find_one({"petition_id": petition_id, "user_id": user_id}, {"_id": 1})
    if not existing:
        await signatures.insert_one({
            "_id": str(uuid.uuid4()),
            "petition_id": petition_id,
            "user_id": user_id,
            "created_at": datetime.utcnow(),
        })

    refreshed = await _recompute_petition_support_snapshot(petition_id)
    out = _petition_out_from_doc(refreshed)
    return CommunitySupportActionOut(
        action="supported" if not existing else "already_supported",
        petition_id=petition_id,
        user_has_supported=True,
        supporter_count=out.supporter_count,
        next_milestone=out.next_milestone,
        current_milestone=out.current_milestone,
        milestone_progress_pct=out.milestone_progress_pct,
        eligible_for_studio_push=out.eligible_for_studio_push,
    )


@router.delete("/petitions/{petition_ref}/support", response_model=CommunitySupportActionOut)
async def unsupport_petition(petition_ref: str, request: Request, user=Depends(get_current_user)):
    _ensure_rate_limit("unsupport_petition", request, COMMUNITY_SUPPORT_RATE_LIMIT)

    petition = await _get_petition_by_ref(petition_ref)
    if not petition:
        raise HTTPException(status_code=404, detail="Petition not found")

    petition_id = _safe_str(petition.get("_id"))
    user_id = _safe_str(user.get("user_id"))
    await database.db.community_petition_signatures.delete_one({"petition_id": petition_id, "user_id": user_id})

    refreshed = await _recompute_petition_support_snapshot(petition_id)
    out = _petition_out_from_doc(refreshed)
    return CommunitySupportActionOut(
        action="unsupported",
        petition_id=petition_id,
        user_has_supported=False,
        supporter_count=out.supporter_count,
        next_milestone=out.next_milestone,
        current_milestone=out.current_milestone,
        milestone_progress_pct=out.milestone_progress_pct,
        eligible_for_studio_push=out.eligible_for_studio_push,
    )
