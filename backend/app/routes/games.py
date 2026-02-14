from datetime import datetime
from typing import Any, Dict, List
import uuid

from fastapi import APIRouter, Depends, HTTPException

from .. import database
from ..models import Game, GameCreate
from .auth import get_current_user

router = APIRouter()


def _game_from_doc(doc: Dict[str, Any]) -> Game:
    return Game(
        id=str(doc.get("_id") or doc.get("id")),
        name=doc.get("name", ""),
        subreddit=doc.get("subreddit", ""),
        keywords=doc.get("keywords"),
        user_id=doc.get("user_id", ""),
        created_at=doc.get("created_at"),
    )


@router.get("", response_model=List[Game])
async def list_games(user=Depends(get_current_user)):
    cursor = database.db.tracked_games.find({"user_id": user["user_id"]})
    games: List[Game] = []
    async for g in cursor:
        games.append(_game_from_doc(g))
    return games


@router.post("", response_model=Game)
async def add_game(game: GameCreate, user=Depends(get_current_user)):
    doc = game.dict()
    doc["user_id"] = user["user_id"]
    doc["created_at"] = datetime.utcnow()
    doc["_id"] = str(uuid.uuid4())

    await database.db.tracked_games.insert_one(doc)
    return _game_from_doc(doc)


@router.get("/{id}", response_model=Game)
async def get_game(id: str, user=Depends(get_current_user)):
    g = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not g:
        raise HTTPException(status_code=404)
    return _game_from_doc(g)


@router.put("/{id}", response_model=Game)
async def update_game(id: str, game: GameCreate, user=Depends(get_current_user)):
    res = await database.db.tracked_games.update_one(
        {"_id": id, "user_id": user["user_id"]}, {"$set": game.dict()}
    )
    if res.matched_count == 0:
        raise HTTPException(status_code=404)

    g = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not g:
        raise HTTPException(status_code=404)
    return _game_from_doc(g)


@router.delete("/{id}")
async def delete_game(id: str, user=Depends(get_current_user)):
    res = await database.db.tracked_games.delete_one({"_id": id, "user_id": user["user_id"]})
    if res.deleted_count == 0:
        raise HTTPException(status_code=404)
    return {"message": "deleted"}
