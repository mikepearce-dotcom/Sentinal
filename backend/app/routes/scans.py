from datetime import datetime
from typing import Any, Dict, List
import uuid

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field

from .. import database, services
from ..models import ScanResultDetailOut, ScanResultOut
from .auth import get_current_user

router = APIRouter()


class MultiScanRequest(BaseModel):
    subreddits: List[str] = Field(default_factory=list, min_items=1, max_items=5)
    game_name: str = ""
    keywords: str = ""
    include_breakdown: bool = True


def _safe_list(value: Any) -> List[Dict[str, Any]]:
    return value if isinstance(value, list) else []


def _scan_out_from_doc(doc: Dict[str, Any]) -> ScanResultOut:
    posts = _safe_list(doc.get("posts"))
    comments = _safe_list(doc.get("comments"))
    return ScanResultOut(
        id=str(doc.get("_id") or doc.get("id")),
        created_at=doc.get("created_at"),
        analysis=doc.get("analysis") or {},
        posts_count=len(posts),
        comments_count=len(comments),
    )


def _scan_detail_out_from_doc(doc: Dict[str, Any]) -> ScanResultDetailOut:
    return ScanResultDetailOut(
        id=str(doc.get("_id") or doc.get("id")),
        created_at=doc.get("created_at"),
        analysis=doc.get("analysis") or {},
        posts=_safe_list(doc.get("posts")),
        comments=_safe_list(doc.get("comments")),
    )


@router.post("/multi-scan")
async def run_multi_scan(payload: MultiScanRequest, user=Depends(get_current_user)):
    if not payload.subreddits:
        raise HTTPException(status_code=400, detail="At least one subreddit is required")

    try:
        result = await services.scan_multiple_subreddits(
            subreddits=payload.subreddits,
            game_name=str(payload.game_name or ""),
            keywords=str(payload.keywords or ""),
            include_breakdown=bool(payload.include_breakdown),
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

    return result


@router.post("/{id}/scan")
async def run_scan(id: str, user=Depends(get_current_user)):
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
        "created_at": datetime.utcnow(),
        "posts": posts,
        "comments": comments,
        "analysis": analysis,
    }

    await database.db.scan_results.insert_one(result)
    return {"message": "scan complete", "result_id": result["_id"]}


@router.get("/{id}/results", response_model=List[ScanResultOut])
async def list_results(id: str, user=Depends(get_current_user)):
    game = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    cursor = database.db.scan_results.find({"game_id": id}).sort("created_at", -1)
    results: List[ScanResultOut] = []
    async for r in cursor:
        results.append(_scan_out_from_doc(r))
    return results


@router.get("/{id}/latest-result", response_model=ScanResultOut)
async def latest_result(id: str, user=Depends(get_current_user)):
    game = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    r = await database.db.scan_results.find_one({"game_id": id}, sort=[("created_at", -1)])
    if not r:
        raise HTTPException(status_code=404)
    return _scan_out_from_doc(r)


@router.get("/{id}/latest-result-detail", response_model=ScanResultDetailOut)
async def latest_result_detail(id: str, user=Depends(get_current_user)):
    game = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    r = await database.db.scan_results.find_one({"game_id": id}, sort=[("created_at", -1)])
    if not r:
        raise HTTPException(status_code=404, detail="No scan results yet")
    return _scan_detail_out_from_doc(r)
