from datetime import datetime
from pydantic import BaseModel, EmailStr, Field
from typing import Any, Dict, List, Optional


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
    scan_type: Optional[str] = None


class ScanResultDetailOut(BaseModel):
    id: str
    created_at: datetime
    analysis: dict
    posts: List[dict]
    comments: List[dict]
    scan_type: Optional[str] = None
    subreddit_breakdown: Optional[dict] = None
    meta: Optional[dict] = None


class ScanCompareOut(BaseModel):
    from_result: ScanResultOut
    to_result: ScanResultOut
    sentiment_from: str = "Unknown"
    sentiment_to: str = "Unknown"
    sentiment_score_delta: int = 0
    posts_delta: int = 0
    comments_delta: int = 0
    summary: Dict[str, Any] = Field(default_factory=dict)
    theme_changes: Dict[str, Any] = Field(default_factory=dict)
    pain_point_changes: Dict[str, Any] = Field(default_factory=dict)
    win_changes: Dict[str, Any] = Field(default_factory=dict)
    subreddit_sentiment_changes: List[Dict[str, Any]] = Field(default_factory=list)


class ScanTrendPointOut(BaseModel):
    id: str
    created_at: datetime
    sentiment_label: str = "Unknown"
    sentiment_score: int = 0
    posts_count: int = 0
    comments_count: int = 0
    scan_type: Optional[str] = None


class ScanTrendsOut(BaseModel):
    window: str = "30d"
    scan_count: int = 0
    points: List[ScanTrendPointOut] = Field(default_factory=list)
    summary: Dict[str, Any] = Field(default_factory=dict)


class Token(BaseModel):
    access_token: str
    token_type: str = "bearer"
