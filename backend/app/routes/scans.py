from datetime import datetime
from typing import Any, Dict, List
import uuid

from fastapi import APIRouter, Depends, HTTPException

from .. import database, services
from ..models import ScanResultDetailOut, ScanResultOut
from .auth import get_current_user

router = APIRouter()


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

    comments: List[Dict[str, Any]] = []
    for p in posts[:5]:
        pid = p.get("id") or p.get("data", {}).get("id")
        if pid:
            try:
                comments += await services.fetch_comments_for_post(pid, limit=20)
            except Exception:
                # Comments are best effort, don't fail whole scan.
                pass

    try:
        analysis = await services.analyze_posts_with_ai(posts, comments)
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

