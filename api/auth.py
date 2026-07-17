"""
安全模块：JWT鉴权 + 密码加密 + 数据脱敏
"""
import os
import time
import hashlib
import secrets
from typing import Optional, Dict
from functools import wraps

import jwt
from cryptography.fernet import Fernet
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

# JWT配置
JWT_SECRET = os.getenv('JWT_SECRET', secrets.token_hex(32))
JWT_ALGORITHM = 'HS256'
JWT_EXPIRY = 86400  # 24小时

# 加密密钥（用于加密数据库密码等敏感配置）
FERNET_KEY = os.getenv('FERNET_KEY')
if not FERNET_KEY:
    FERNET_KEY = Fernet.generate_key().decode()
    print(f"[WARN] 未设置FERNET_KEY，已自动生成: {FERNET_KEY}")
    print("[WARN] 请将此key写入.env文件的FERNET_KEY项")

# Ensure key is valid for Fernet (32 url-safe base64-encoded bytes)
try:
    _fernet = Fernet(FERNET_KEY.encode())
except ValueError:
    # If key is invalid, generate a new one
    FERNET_KEY = Fernet.generate_key().decode()
    _fernet = Fernet(FERNET_KEY.encode())
    print(f"[WARN] 原FERNET_KEY无效，已自动生成新key: {FERNET_KEY}")


# ==================== JWT 鉴权 ====================

def create_token(user_id: str = "admin", role: str = "analyst") -> str:
    """创建JWT token"""
    payload = {
        "sub": user_id,
        "role": role,
        "iat": int(time.time()),
        "exp": int(time.time()) + JWT_EXPIRY,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def verify_token(token: str) -> Optional[Dict]:
    """验证JWT token，返回payload或None"""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


def require_auth(func):
    """FastAPI依赖：要求JWT鉴权"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        from fastapi import Request, HTTPException
        request = kwargs.get('request') or args[0] if args else None
        if request is None:
            for v in kwargs.values():
                if hasattr(v, 'headers'):
                    request = v
                    break
            if request is None:
                raise HTTPException(status_code=401, detail="无法获取请求上下文")

        auth_header = request.headers.get("Authorization", "")
        if not auth_header.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="缺少有效的认证token")

        token = auth_header[7:]
        payload = verify_token(token)
        if payload is None:
            raise HTTPException(status_code=401, detail="Token无效或已过期")

        request.state.user = payload
        return await func(*args, **kwargs)
    return wrapper


def optional_auth(func):
    """FastAPI依赖：可选JWT鉴权（不强制）"""
    @wraps(func)
    async def wrapper(*args, **kwargs):
        from fastapi import Request
        request = kwargs.get('request') or args[0] if args else None
        if request is None:
            for v in kwargs.values():
                if hasattr(v, 'headers'):
                    request = v
                    break

        user = None
        if request:
            auth_header = request.headers.get("Authorization", "")
            if auth_header.startswith("Bearer "):
                user = verify_token(auth_header[7:])
        if request:
            request.state.user = user
        return await func(*args, **kwargs)
    return wrapper


# ==================== 密码加密 ====================

def encrypt_password(password: str) -> str:
    """加密密码（单向哈希+盐）"""
    salt = secrets.token_hex(8)
    hashed = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000).hex()
    return f"{salt}${hashed}"


def verify_password(password: str, stored: str) -> bool:
    """验证密码"""
    try:
        salt, hashed = stored.split('$')
        new_hashed = hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000).hex()
        return new_hashed == hashed
    except ValueError:
        return False


# ==================== 配置加密 ====================

def encrypt_config(plaintext: str) -> str:
    """加密配置值（可逆，用于数据库密码等）"""
    return _fernet.encrypt(plaintext.encode()).decode()


def decrypt_config(ciphertext: str) -> str:
    """解密配置值"""
    return _fernet.decrypt(ciphertext.encode()).decode()


# ==================== 数据脱敏 ====================

def mask_account(account_id: str) -> str:
    """脱敏账户ID：ACCT318364 → ACCT****8364"""
    if not account_id or len(account_id) < 6:
        return account_id
    return account_id[:4] + '****' + account_id[-4:]


def mask_phone(phone: str) -> str:
    """脱敏手机号：13812345678 → 138****5678"""
    if not phone or len(phone) < 7:
        return phone
    return phone[:3] + '****' + phone[-4:]


def mask_device(device_id: str) -> str:
    """脱敏设备ID"""
    if not device_id or len(device_id) < 8:
        return device_id
    return device_id[:4] + '****' + device_id[-4:]


def mask_amount(amount: float, threshold: float = 10000) -> float:
    """保留金额数值，不再脱敏为范围字符串"""
    return float(amount)


def sanitize_alert(alert: dict, level: str = "full") -> dict:
    """
    脱敏告警数据
    level: 'full' 完全脱敏, 'partial' 部分脱敏(保留后4位)
    """
    result = dict(alert)
    if result.get('account_id'):
        result['account_id'] = mask_account(result['account_id'])
    if result.get('device_id'):
        result['device_id'] = mask_device(result['device_id'])
    if result.get('amount') is not None:
        try:
            result['amount'] = float(result['amount'])
        except (ValueError, TypeError):
            result['amount'] = 0.0
    return result


def sanitize_alert_list(alerts: list, level: str = "full") -> list:
    """批量脱敏告警列表"""
    return [sanitize_alert(a, level) for a in alerts]
