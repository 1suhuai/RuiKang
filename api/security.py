"""
JWT认证模块 - 令牌生成、验证与用户依赖注入
"""
import sys
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

# 确保 .tmp_libs 在 sys.path 中
_libs_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), ".tmp_libs")
if _libs_dir not in sys.path:
    sys.path.insert(0, _libs_dir)

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt

from config import settings

# Bearer token scheme
security = HTTPBearer()

# ============================================================
# Token 操作
# ============================================================

def create_access_token(data: dict, expires_delta: Optional[timedelta] = None) -> str:
    """生成 JWT access token"""
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.now(timezone.utc) + expires_delta
    else:
        expire = datetime.now(timezone.utc) + timedelta(minutes=settings.ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, settings.SECRET_KEY, algorithm=settings.ALGORITHM)
    return encoded_jwt


def get_current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    """
    验证 JWT token 并返回当前用户信息
    作为 FastAPI Depends 依赖项使用
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="无效的认证凭据",
        headers={"WWW-Authenticate": "Bearer"},
    )
    try:
        payload = jwt.decode(
            credentials.credentials,
            settings.SECRET_KEY,
            algorithms=[settings.ALGORITHM],
        )
        username: str = payload.get("sub")
        if username is None:
            raise credentials_exception
        return {"username": username, "sub": payload.get("sub")}
    except JWTError:
        raise credentials_exception
