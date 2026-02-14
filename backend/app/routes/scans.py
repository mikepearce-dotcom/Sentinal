from fastapi import APIRouter, HTTPException, Depends
from .. import database
from ..models import ScanResultOut
from typing import List
from datetime import datetime
import uuid

from .. import services

router = APIRouter()

# reuse auth dependency
from .auth import get_current_user


@router.post("/{id}/scan")
async def run_scan(id: str, user=Depends(get_current_user)):
    # validate game belongs to user
    game = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")
    subreddit = game.get("subreddit")
    posts = await services.fetch_reddit_posts(subreddit, limit=100)
    comments = []
    # attempt to fetch comments for each post (limiting to first 5 posts)
    for p in posts[:5]:
        pid = p.get("id") or p.get("data", {}).get("id")
        if pid:
            try:
                comments += await services.fetch_comments_for_post(pid, limit=20)
            except Exception:
                pass

    analysis = await services.analyze_posts_with_ai(posts, comments)
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
    cursor = database.db.scan_results.find({"game_id": id})
    results = []
    async for r in cursor:
        results.append(ScanResultOut(**r))
    return results


@router.get("/{id}/latest-result", response_model=ScanResultOut)
async def latest_result(id: str, user=Depends(get_current_user)):
    r = await database.db.scan_results.find_one({"game_id": id}, sort=[("created_at", -1)])
    if not r:
        raise HTTPException(status_code=404)
    return ScanResultOut(**r)

