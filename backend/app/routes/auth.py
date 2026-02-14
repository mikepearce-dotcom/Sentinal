from fastapi import APIRouter, HTTPException, Depends, status
from pydantic import BaseModel
from ..models import UserCreate, UserBase, Token
from ..database import db
from ..utils import hash_password, verify_password
import uuid
import os
import jwt
from datetime import datetime, timedelta

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
    # check if email exists
    existing = await db.users.find_one({"email": user.email})
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    user_dict = user.dict()
    user_dict["password_hash"] = hash_password(user_dict.pop("password"))
    user_dict["user_id"] = str(uuid.uuid4())
    from datetime import datetime
    user_dict["created_at"] = datetime.utcnow()
    await db.users.insert_one(user_dict)
    return {"message": "user created"}


@router.post("/login", response_model=Token)
async def login(data: LoginData):
    user = await db.users.find_one({"email": data.email})
    if not user or not verify_password(data.password, user["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid email or password")
    access_token = create_access_token(user_id=user["user_id"])
    return {"access_token": access_token, "token_type": "bearer"}


from fastapi import Header


async def get_current_user(authorization: str | None = Header(None)):
    # expect header "Authorization: Bearer <token>"
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Unauthorized")
    token = authorization.split(" ", 1)[1]
    data = decode_access_token(token)
    user_id = data.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Unauthorized")
    user = await db.users.find_one({"user_id": user_id})
    if not user:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return user


@router.get("/me")
async def me(user=Depends(get_current_user)):
    if user:
        return {"email": user["email"], "name": user["name"], "user_id": user["user_id"]}
    raise HTTPException(status_code=401)


# logout endpoint is effectively a no-op with JWT (client just discards token)
@router.post("/logout")
async def logout():
    return {"message": "logged out"}
