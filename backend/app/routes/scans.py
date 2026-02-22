import os
import re
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from .. import database, services
from ..models import ScanCompareOut, ScanResultDetailOut, ScanResultOut, ScanTrendPointOut, ScanTrendsOut
from ..security import allow_request, client_ip, parse_int_env
from .auth import get_current_user

router = APIRouter()

SCAN_RATE_LIMIT = parse_int_env(os.getenv("SCAN_RATE_LIMIT"), default=20)
SCAN_RATE_WINDOW_SECONDS = parse_int_env(os.getenv("SCAN_RATE_WINDOW_SECONDS"), default=300)


class MultiScanRequest(BaseModel):
    subreddits: List[str] = Field(default_factory=list, min_items=1, max_items=5)
    game_id: str = ""
    game_name: str = ""
    keywords: str = ""
    include_breakdown: bool = True


def _safe_list(value: Any) -> List[Dict[str, Any]]:
    return value if isinstance(value, list) else []


def _enforce_scan_rate_limit(request: Request, user_id: str, scope: str) -> None:
    key = f"scan:{scope}:{user_id}:{client_ip(request)}"
    if allow_request(key, limit=SCAN_RATE_LIMIT, window_seconds=SCAN_RATE_WINDOW_SECONDS):
        return

    raise HTTPException(status_code=429, detail="Too many scan requests. Please wait and try again.")


def _scan_filter_for_user_game(game_id: str, user_id: str) -> Dict[str, Any]:
    # Keep legacy compatibility for historical rows that predate user_id tagging.
    return {
        "game_id": game_id,
        "$or": [
            {"user_id": user_id},
            {"user_id": {"$exists": False}},
        ],
    }


def _scan_out_from_doc(doc: Dict[str, Any]) -> ScanResultOut:
    posts = _safe_list(doc.get("posts"))
    comments = _safe_list(doc.get("comments"))
    return ScanResultOut(
        id=str(doc.get("_id") or doc.get("id")),
        created_at=doc.get("created_at"),
        analysis=doc.get("analysis") or {},
        posts_count=len(posts),
        comments_count=len(comments),
        scan_type=(str(doc.get("scan_type") or "").strip() or None),
    )


def _scan_detail_out_from_doc(doc: Dict[str, Any]) -> ScanResultDetailOut:
    return ScanResultDetailOut(
        id=str(doc.get("_id") or doc.get("id")),
        created_at=doc.get("created_at"),
        analysis=doc.get("analysis") or {},
        posts=_safe_list(doc.get("posts")),
        comments=_safe_list(doc.get("comments")),
        scan_type=(str(doc.get("scan_type") or "").strip() or None),
        subreddit_breakdown=(
            doc.get("subreddit_breakdown") if isinstance(doc.get("subreddit_breakdown"), dict) else None
        ),
        meta=(doc.get("meta") if isinstance(doc.get("meta"), dict) else None),
    )


def _doc_created_at(doc: Dict[str, Any]) -> Optional[datetime]:
    value = doc.get("created_at")
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        candidate = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(candidate)
            if parsed.tzinfo is not None:
                return parsed.replace(tzinfo=None)
            return parsed
        except Exception:
            return None
    return None


def _sentiment_score(label: str) -> int:
    value = str(label or "").strip().lower()
    if "positive" in value:
        return 1
    if "negative" in value:
        return -1
    return 0


def _sentiment_direction(delta: int) -> str:
    if delta > 0:
        return "improved"
    if delta < 0:
        return "declined"
    return "unchanged"


def _analysis_list_texts(analysis: Any, key: str, max_items: int = 10) -> List[str]:
    if not isinstance(analysis, dict):
        return []
    raw = analysis.get(key)
    if not isinstance(raw, list):
        return []

    items: List[str] = []
    for entry in raw:
        text_value = ""
        if isinstance(entry, str):
            text_value = entry.strip()
        elif isinstance(entry, dict):
            text_value = str(
                entry.get("text")
                or entry.get("summary")
                or entry.get("point")
                or entry.get("title")
                or ""
            ).strip()
        else:
            text_value = str(entry or "").strip()

        if text_value and text_value not in items:
            items.append(text_value)
        if len(items) >= max(max_items, 1):
            break

    return items


def _normalize_compare_key(text: str, prefer_prefix: bool = False) -> str:
    value = str(text or "")
    if prefer_prefix and " - " in value:
        value = value.split(" - ", 1)[0]

    value = re.sub(r"\[POST:[A-Za-z0-9_]+\]", " ", value)
    value = re.sub(r"https?://\S+", " ", value)
    tokens = [token for token in re.findall(r"[a-z0-9]+", value.lower()) if len(token) >= 3]
    if not tokens:
        return ""
    max_tokens = 8 if prefer_prefix else 16
    return " ".join(tokens[:max_tokens])


def _build_item_change_set(
    from_items: List[str],
    to_items: List[str],
    *,
    prefer_prefix: bool = False,
    max_samples: int = 5,
) -> Dict[str, Any]:
    def _to_map(items: List[str]) -> Dict[str, str]:
        mapped: Dict[str, str] = {}
        for item in items:
            key = _normalize_compare_key(item, prefer_prefix=prefer_prefix)
            if not key:
                continue
            if key not in mapped:
                mapped[key] = item
        return mapped

    from_map = _to_map(from_items)
    to_map = _to_map(to_items)

    new_keys = [key for key in to_map.keys() if key not in from_map]
    removed_keys = [key for key in from_map.keys() if key not in to_map]
    persisting_keys = [key for key in to_map.keys() if key in from_map]

    return {
        "new": [to_map[key] for key in new_keys[:max_samples]],
        "removed": [from_map[key] for key in removed_keys[:max_samples]],
        "persisting": [to_map[key] for key in persisting_keys[:max_samples]],
        "new_count": len(new_keys),
        "removed_count": len(removed_keys),
        "persisting_count": len(persisting_keys),
    }


def _subreddit_sentiment_map(doc: Dict[str, Any]) -> Dict[str, str]:
    breakdown = doc.get("subreddit_breakdown")
    if not isinstance(breakdown, dict):
        return {}
    rows = breakdown.get("breakdown")
    if not isinstance(rows, list):
        return {}

    result: Dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        subreddit = str(row.get("subreddit") or "").strip().lower()
        if not subreddit:
            continue
        result[subreddit] = str(row.get("sentiment_label") or "Unknown")
    return result


def _build_subreddit_sentiment_changes(from_doc: Dict[str, Any], to_doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    from_map = _subreddit_sentiment_map(from_doc)
    to_map = _subreddit_sentiment_map(to_doc)
    if not from_map and not to_map:
        return []

    rows: List[Dict[str, Any]] = []
    for subreddit in sorted(set(from_map.keys()) | set(to_map.keys())):
        from_label = from_map.get(subreddit, "Not present")
        to_label = to_map.get(subreddit, "Not present")
        delta = _sentiment_score(to_label) - _sentiment_score(from_label)
        if subreddit in from_map and subreddit in to_map and from_label == to_label:
            continue

        rows.append(
            {
                "subreddit": subreddit,
                "from_sentiment": from_label,
                "to_sentiment": to_label,
                "delta": delta,
                "direction": _sentiment_direction(delta),
            }
        )

    rows.sort(key=lambda item: (-abs(int(item.get("delta", 0) or 0)), str(item.get("subreddit") or "")))
    return rows[:10]


def _build_compare_payload(from_doc: Dict[str, Any], to_doc: Dict[str, Any]) -> ScanCompareOut:
    from_out = _scan_out_from_doc(from_doc)
    to_out = _scan_out_from_doc(to_doc)

    from_analysis = from_doc.get("analysis") if isinstance(from_doc.get("analysis"), dict) else {}
    to_analysis = to_doc.get("analysis") if isinstance(to_doc.get("analysis"), dict) else {}

    sentiment_from = str(from_analysis.get("sentiment_label") or "Unknown")
    sentiment_to = str(to_analysis.get("sentiment_label") or "Unknown")
    sentiment_delta = _sentiment_score(sentiment_to) - _sentiment_score(sentiment_from)

    posts_delta = int(to_out.posts_count or 0) - int(from_out.posts_count or 0)
    comments_delta = int(to_out.comments_count or 0) - int(from_out.comments_count or 0)

    from_created_at = _doc_created_at(from_doc)
    to_created_at = _doc_created_at(to_doc)
    days_between = 0.0
    if from_created_at and to_created_at:
        days_between = round(max((to_created_at - from_created_at).total_seconds(), 0.0) / 86400.0, 2)

    theme_changes = _build_item_change_set(
        _analysis_list_texts(from_analysis, "themes", max_items=12),
        _analysis_list_texts(to_analysis, "themes", max_items=12),
        prefer_prefix=True,
    )
    pain_point_changes = _build_item_change_set(
        _analysis_list_texts(from_analysis, "pain_points", max_items=8),
        _analysis_list_texts(to_analysis, "pain_points", max_items=8),
    )
    win_changes = _build_item_change_set(
        _analysis_list_texts(from_analysis, "wins", max_items=8),
        _analysis_list_texts(to_analysis, "wins", max_items=8),
    )

    return ScanCompareOut(
        from_result=from_out,
        to_result=to_out,
        sentiment_from=sentiment_from,
        sentiment_to=sentiment_to,
        sentiment_score_delta=sentiment_delta,
        posts_delta=posts_delta,
        comments_delta=comments_delta,
        summary={
            "direction": _sentiment_direction(sentiment_delta),
            "days_between": days_between,
            "scan_type_from": from_out.scan_type or "single",
            "scan_type_to": to_out.scan_type or "single",
            "theme_new_count": int(theme_changes.get("new_count", 0) or 0),
            "theme_removed_count": int(theme_changes.get("removed_count", 0) or 0),
            "pain_new_count": int(pain_point_changes.get("new_count", 0) or 0),
            "pain_removed_count": int(pain_point_changes.get("removed_count", 0) or 0),
            "win_new_count": int(win_changes.get("new_count", 0) or 0),
            "win_removed_count": int(win_changes.get("removed_count", 0) or 0),
        },
        theme_changes=theme_changes,
        pain_point_changes=pain_point_changes,
        win_changes=win_changes,
        subreddit_sentiment_changes=_build_subreddit_sentiment_changes(from_doc, to_doc),
    )


def _parse_window_days(window: str) -> int:
    value = str(window or "30d").strip().lower()
    match = re.fullmatch(r"(\d{1,3})d", value)
    if not match:
        raise HTTPException(status_code=400, detail="Invalid window. Use formats like 7d, 30d, 90d.")

    days = int(match.group(1))
    if days < 1 or days > 365:
        raise HTTPException(status_code=400, detail="Window must be between 1d and 365d.")
    return days


@router.post("/multi-scan")
async def run_multi_scan(payload: MultiScanRequest, request: Request, user=Depends(get_current_user)):
    _enforce_scan_rate_limit(request, user["user_id"], scope="multi")

    if not payload.subreddits:
        raise HTTPException(status_code=400, detail="At least one subreddit is required")

    try:
        result = await services.scan_multiple_subreddits(
            subreddits=payload.subreddits,
            game_name=str(payload.game_name or ""),
            keywords=str(payload.keywords or ""),
            include_breakdown=bool(payload.include_breakdown),
            include_internal=bool(str(payload.game_id or "").strip()),
        )
    except RuntimeError as exc:
        detail = str(exc)
        lowered = detail.lower()
        if "at least one valid subreddit" in lowered:
            raise HTTPException(status_code=400, detail=detail)
        if "no posts found" in lowered:
            raise HTTPException(status_code=404, detail=detail)
        raise HTTPException(status_code=500, detail=detail)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Multi scan failed: {exc}")

    game_id = str(payload.game_id or "").strip()
    if game_id:
        game = await database.db.tracked_games.find_one({"_id": game_id, "user_id": user["user_id"]})
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")

        scan_doc = {
            "_id": str(uuid.uuid4()),
            "game_id": game_id,
            "user_id": user["user_id"],
            "created_at": datetime.utcnow(),
            "posts": _safe_list(result.get("_posts")),
            "comments": _safe_list(result.get("_comments")),
            "analysis": result.get("overall") or {},
            "scan_type": "multi",
            "subreddit_breakdown": result.get("subreddit_breakdown") or {"breakdown": []},
            "meta": result.get("meta") or {},
        }
        await database.db.scan_results.insert_one(scan_doc)

    return {
        "overall": result.get("overall") or {},
        "meta": result.get("meta") or {},
        "subreddit_breakdown": result.get("subreddit_breakdown") or {"breakdown": []},
    }


@router.post("/{id}/scan")
async def run_scan(id: str, request: Request, user=Depends(get_current_user)):
    _enforce_scan_rate_limit(request, user["user_id"], scope="single")

    game = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    subreddit = game.get("subreddit", "")

    try:
        posts = await services.fetch_reddit_posts(subreddit, limit=100)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch Reddit posts: {exc}")

    if not posts:
        raise HTTPException(
            status_code=404,
            detail=f"No posts found for r/{subreddit}. Check subreddit name or try again later.",
        )

    try:
        comments = await services.sample_comments_for_posts(
            posts,
            max_posts=services.TOP_POSTS_FOR_COMMENTS,
            max_comments_per_post=services.MAX_COMMENTS_PER_POST,
        )
    except Exception:
        comments = []

    try:
        analysis = await services.analyze_posts_with_ai(
            posts,
            comments,
            game_name=str(game.get("name", "") or ""),
            keywords=str(game.get("keywords", "") or ""),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"AI analysis failed: {exc}")

    result = {
        "_id": str(uuid.uuid4()),
        "game_id": id,
        "user_id": user["user_id"],
        "created_at": datetime.utcnow(),
        "posts": posts,
        "comments": comments,
        "analysis": analysis,
        "scan_type": "single",
    }

    await database.db.scan_results.insert_one(result)
    return {"message": "scan complete", "result_id": result["_id"]}


@router.get("/{id}/results", response_model=List[ScanResultOut])
async def list_results(id: str, user=Depends(get_current_user)):
    game = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    cursor = database.db.scan_results.find(_scan_filter_for_user_game(id, user["user_id"])).sort("created_at", -1)
    results: List[ScanResultOut] = []
    async for r in cursor:
        results.append(_scan_out_from_doc(r))
    return results


@router.get("/{id}/results/compare", response_model=ScanCompareOut)
async def compare_results(
    id: str,
    from_result_id: Optional[str] = None,
    to_result_id: Optional[str] = None,
    user=Depends(get_current_user),
):
    game = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    cursor = (
        database.db.scan_results
        .find(_scan_filter_for_user_game(id, user["user_id"]))
        .sort("created_at", -1)
        .limit(250)
    )
    docs: List[Dict[str, Any]] = []
    async for row in cursor:
        docs.append(row)

    if len(docs) < 2:
        raise HTTPException(status_code=404, detail="Need at least two scans to compare")

    by_id = {str(doc.get("_id") or doc.get("id")): doc for doc in docs}

    if to_result_id:
        to_key = str(to_result_id).strip()
        to_doc = by_id.get(to_key)
        if not to_doc:
            raise HTTPException(status_code=404, detail="Target scan not found")
        try:
            to_idx = next(idx for idx, doc in enumerate(docs) if str(doc.get("_id") or doc.get("id")) == to_key)
        except StopIteration:
            raise HTTPException(status_code=404, detail="Target scan not found")
    else:
        to_doc = docs[0]
        to_idx = 0

    if from_result_id:
        from_key = str(from_result_id).strip()
        from_doc = by_id.get(from_key)
        if not from_doc:
            raise HTTPException(status_code=404, detail="Baseline scan not found")
    else:
        if to_idx + 1 >= len(docs):
            raise HTTPException(status_code=404, detail="No previous scan available for comparison")
        from_doc = docs[to_idx + 1]

    if str(from_doc.get("_id") or from_doc.get("id")) == str(to_doc.get("_id") or to_doc.get("id")):
        raise HTTPException(status_code=400, detail="Cannot compare a scan to itself")

    from_dt = _doc_created_at(from_doc)
    to_dt = _doc_created_at(to_doc)
    if from_dt and to_dt and from_dt > to_dt:
        from_doc, to_doc = to_doc, from_doc

    return _build_compare_payload(from_doc, to_doc)


@router.get("/{id}/results/trends", response_model=ScanTrendsOut)
async def scan_trends(
    id: str,
    window: str = "30d",
    limit: int = 120,
    user=Depends(get_current_user),
):
    game = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    days = _parse_window_days(window)
    safe_limit = max(1, min(int(limit or 120), 500))
    cutoff = datetime.utcnow() - timedelta(days=days)

    query = _scan_filter_for_user_game(id, user["user_id"])
    query["created_at"] = {"$gte": cutoff}

    cursor = database.db.scan_results.find(query).sort("created_at", -1).limit(safe_limit)
    docs_desc: List[Dict[str, Any]] = []
    async for row in cursor:
        docs_desc.append(row)

    docs_asc = list(reversed(docs_desc))
    points: List[ScanTrendPointOut] = []
    sentiment_counts = {"Positive": 0, "Mixed": 0, "Negative": 0, "Unknown": 0}
    total_posts = 0
    total_comments = 0

    for doc in docs_asc:
        out = _scan_out_from_doc(doc)
        label = str((doc.get("analysis") or {}).get("sentiment_label") or "Unknown")
        score = _sentiment_score(label)
        normalized_label = "Positive" if score > 0 else "Negative" if score < 0 else ("Mixed" if "mixed" in label.lower() else "Unknown")
        sentiment_counts[normalized_label] = int(sentiment_counts.get(normalized_label, 0) or 0) + 1

        posts_count = int(out.posts_count or 0)
        comments_count = int(out.comments_count or 0)
        total_posts += posts_count
        total_comments += comments_count

        points.append(
            ScanTrendPointOut(
                id=out.id,
                created_at=out.created_at,
                sentiment_label=label,
                sentiment_score=score,
                posts_count=posts_count,
                comments_count=comments_count,
                scan_type=out.scan_type,
            )
        )

    summary: Dict[str, Any] = {
        "window_days": days,
        "sentiment_counts": sentiment_counts,
        "total_posts": total_posts,
        "total_comments": total_comments,
        "average_posts_per_scan": round(total_posts / len(points), 2) if points else 0.0,
        "average_comments_per_scan": round(total_comments / len(points), 2) if points else 0.0,
        "latest_sentiment": points[-1].sentiment_label if points else "Unknown",
        "oldest_sentiment": points[0].sentiment_label if points else "Unknown",
    }

    if len(docs_desc) >= 2:
        latest_vs_previous = _build_compare_payload(docs_desc[1], docs_desc[0])
        summary["latest_vs_previous"] = {
            "from_result_id": latest_vs_previous.from_result.id,
            "to_result_id": latest_vs_previous.to_result.id,
            "direction": latest_vs_previous.summary.get("direction"),
            "sentiment_from": latest_vs_previous.sentiment_from,
            "sentiment_to": latest_vs_previous.sentiment_to,
            "sentiment_score_delta": latest_vs_previous.sentiment_score_delta,
            "posts_delta": latest_vs_previous.posts_delta,
            "comments_delta": latest_vs_previous.comments_delta,
        }

    return ScanTrendsOut(window=f"{days}d", scan_count=len(points), points=points, summary=summary)


@router.get("/{id}/results/{result_id}/detail", response_model=ScanResultDetailOut)
async def result_detail(id: str, result_id: str, user=Depends(get_current_user)):
    game = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    query = _scan_filter_for_user_game(id, user["user_id"])
    query["_id"] = result_id
    r = await database.db.scan_results.find_one(query)
    if not r:
        raise HTTPException(status_code=404, detail="Scan result not found")

    return _scan_detail_out_from_doc(r)


@router.get("/{id}/latest-result", response_model=ScanResultOut)
async def latest_result(id: str, user=Depends(get_current_user)):
    game = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    r = await database.db.scan_results.find_one(
        _scan_filter_for_user_game(id, user["user_id"]),
        sort=[("created_at", -1)],
    )
    if not r:
        raise HTTPException(status_code=404)
    return _scan_out_from_doc(r)


@router.get("/{id}/latest-result-detail", response_model=ScanResultDetailOut)
async def latest_result_detail(id: str, user=Depends(get_current_user)):
    game = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    r = await database.db.scan_results.find_one(
        _scan_filter_for_user_game(id, user["user_id"]),
        sort=[("created_at", -1)],
    )
    if not r:
        raise HTTPException(status_code=404, detail="No scan results yet")
    return _scan_detail_out_from_doc(r)
