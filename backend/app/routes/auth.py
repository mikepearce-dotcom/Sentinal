import os
import uuid
import jwt
from datetime import datetime, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from .. import database
from ..models import Token, UserCreate
from ..utils import hash_password, verify_password

router = APIRouter()

# jwt utilities
JWT_SECRET = os.getenv("JWT_SECRET", "change-me")
JWT_ALGORITHM = "HS256"
TOKEN_EXPIRE_MINUTES = 60


def create_access_token(user_id: str) -> str:
    expire = datetime.utcnow() + timedelta(minutes=TOKEN_EXPIRE_MINUTES)
    to_encode = {"user_id": user_id, "exp": expire}
    return jwt.encode(to_encode, JWT_SECRET, algorithm=JWT_ALGORITHM)


def decode_access_token(token: str) -> dict:
    try:
        return jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.PyJWTError:
        return {}


class LoginData(BaseModel):
    email: str
    password: str


@router.post("/signup")
async def signup(user: UserCreate):
    existing = await database.db.users.find_one({"email": user.email})
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")

    user_dict = user.dict()
    user_dict["password_hash"] = hash_password(user_dict.pop("password"))
    user_dict["user_id"] = str(uuid.uuid4())
    user_dict["created_at"] = datetime.utcnow()

    await database.db.users.insert_one(user_dict)
    return {"message": "user created"}


@router.post("/login", response_model=Token)
async def login(data: LoginData):
    user = await database.db.users.find_one({"email": data.email})
    if not user or not verify_password(data.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")

    access_token = create_access_token(user_id=user["user_id"])
    return {"access_token": access_token, "token_type": "bearer"}


async def get_current_user(authorization: Optional[str] = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")

    token = authorization.split(" ", 1)[1]
    data = decode_access_token(token)
    user_id = data.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")

    user = await database.db.users.find_one({"user_id": user_id})
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")

    return user


@router.get("/me")
async def me(user=Depends(get_current_user)):
    if user:
        return {
            "email": user["email"],
            "name": user["name"],
            "user_id": user["user_id"],
        }
    raise HTTPException(status_code=401)


@router.post("/logout")
async def logout():
    return {"message": "logged out"}
