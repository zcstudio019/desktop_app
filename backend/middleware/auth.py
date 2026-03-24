"""
JWT authentication helpers for FastAPI.

Provides token creation/verification plus FastAPI dependencies
for authentication and role-based access control.
"""

import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import jwt
from dotenv import load_dotenv
from fastapi import Depends, Header, HTTPException

logger = logging.getLogger(__name__)

_env_path = Path(__file__).resolve().parents[2] / ".env"
load_dotenv(_env_path)

JWT_ALGORITHM = "HS256"
JWT_EXPIRY_HOURS = 24


def _load_jwt_secret() -> str:
    """Load the JWT signing secret from environment or .env."""
    secret = (os.getenv("JWT_SECRET") or "").strip()
    if not secret:
        raise RuntimeError("JWT_SECRET 未配置，认证服务无法启动。")
    return secret


JWT_SECRET = _load_jwt_secret()


def create_access_token(username: str, role: str) -> str:
    """Create a JWT access token."""
    now = datetime.now(timezone.utc)
    payload = {
        "sub": username,
        "role": role,
        "iat": now,
        "exp": now + timedelta(hours=JWT_EXPIRY_HOURS),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


async def get_current_user(authorization: str = Header(None)) -> dict:
    """Extract and verify the current user from a Bearer token."""
    if not authorization:
        raise HTTPException(status_code=401, detail="未提供认证令牌")

    parts = authorization.split(" ", 1)
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="认证令牌格式错误")

    token = parts[1]

    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="认证令牌已过期") from None
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="无效的认证令牌") from None

    username = payload.get("sub")
    role = payload.get("role")
    if not username or not role:
        raise HTTPException(status_code=401, detail="无效的认证令牌")

    return {"username": username, "role": role}


async def get_current_user_optional(authorization: str = Header(None)) -> dict | None:
    """Return authenticated user info if present, else None."""
    if not authorization:
        return None
    try:
        return await get_current_user(authorization)
    except HTTPException:
        return None


async def require_admin(current_user: dict = Depends(get_current_user)) -> dict:
    """Require the authenticated user to be an admin."""
    if current_user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="需要管理员权限")
    return current_user
