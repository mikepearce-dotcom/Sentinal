from datetime import datetime
from pydantic import BaseModel, EmailStr, Field
from typing import List, Optional


class UserBase(BaseModel):
    email: EmailStr
    name: str


class UserCreate(UserBase):
    password: str


class UserInDB(UserBase):
    user_id: str
    password_hash: Optional[str] = None
    auth_provider: str = "email"
    auth0_sub: Optional[str] = None
    created_at: datetime


class GameBase(BaseModel):
    name: str
    subreddit: str
    keywords: Optional[str] = None


class GameCreate(GameBase):
    pass


class GameInDB(GameBase):
    id: str = Field(..., alias="_id")
    user_id: str
    created_at: datetime


class ScanResult(BaseModel):
    id: str = Field(..., alias="_id")
    game_id: str
    created_at: datetime
    posts: List[dict]
    comments: List[dict]
    analysis: dict


# schemas returned in responses
class Game(GameBase):
    id: str
    user_id: str
    created_at: datetime


class ScanResultOut(BaseModel):
    id: str
    created_at: datetime
    analysis: dict
    posts_count: int = 0
    comments_count: int = 0


class ScanResultDetailOut(BaseModel):
    id: str
    created_at: datetime
    analysis: dict
    posts: List[dict]
    comments: List[dict]


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"
