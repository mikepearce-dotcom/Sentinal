from fastapi import APIRouter, HTTPException, Depends
from typing import List
from ..models import Game, GameCreate
from ..database import db
import uuid
from datetime import datetime

router = APIRouter()

# reuse auth dependency to decode JWT and fetch user
from .auth import get_current_user


@router.get("", response_model=List[Game])
async def list_games(user=Depends(get_current_user)):
    cursor = db.tracked_games.find({"user_id": user["user_id"]})
    games = []
    async for g in cursor:
        games.append(Game(**g))
    return games


@router.post("", response_model=Game)
async def add_game(game: GameCreate, user=Depends(get_current_user)):
    doc = game.dict()
    doc["user_id"] = user["user_id"]
    doc["created_at"] = datetime.utcnow()
    doc["_id"] = str(uuid.uuid4())
    await db.tracked_games.insert_one(doc)
    return Game(**doc)


@router.get("/{id}", response_model=Game)
async def get_game(id: str, user=Depends(get_current_user)):
    g = await db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not g:
        raise HTTPException(status_code=404)
    return Game(**g)


@router.put("/{id}", response_model=Game)
async def update_game(id: str, game: GameCreate, user=Depends(get_current_user)):
    res = await db.tracked_games.update_one(
        {"_id": id, "user_id": user["user_id"]}, {"$set": game.dict()}
    )
    if res.modified_count == 0:
        raise HTTPException(status_code=404)
    g = await db.tracked_games.find_one({"_id": id})
    return Game(**g)


@router.delete("/{id}")
async def delete_game(id: str, user=Depends(get_current_user)):
    res = await db.tracked_games.delete_one({"_id": id, "user_id": user["user_id"]})
    if res.deleted_count == 0:
        raise HTTPException(status_code=404)
    return {"message": "deleted"}
