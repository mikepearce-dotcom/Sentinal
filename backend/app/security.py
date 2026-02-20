import time
from collections import deque
from threading import Lock
from typing import Deque, Dict, Optional

from fastapi import Request

_TRUE_VALUES = {"1", "true", "yes", "on"}


def clean_env(value: Optional[str]) -> str:
    return str(value or "").strip().strip('"').strip("'")


def env_truthy(value: Optional[str], default: bool = False) -> bool:
    cleaned = clean_env(value)
    if not cleaned:
        return default
    return cleaned.lower() in _TRUE_VALUES


def parse_int_env(value: Optional[str], default: int, min_value: int = 1) -> int:
    cleaned = clean_env(value)
    if not cleaned:
        return default

    try:
        parsed = int(cleaned)
    except Exception:
        return default

    return parsed if parsed >= min_value else default


def client_ip(request: Request) -> str:
    x_forwarded_for = clean_env(request.headers.get("x-forwarded-for"))
    if x_forwarded_for:
        first = x_forwarded_for.split(",", 1)[0].strip()
        if first:
            return first

    x_real_ip = clean_env(request.headers.get("x-real-ip"))
    if x_real_ip:
        return x_real_ip

    if request.client and request.client.host:
        return str(request.client.host)

    return "unknown"


_rate_limit_buckets: Dict[str, Deque[float]] = {}
_rate_limit_lock = Lock()


def allow_request(key: str, limit: int, window_seconds: int) -> bool:
    now = time.time()
    cutoff = now - float(window_seconds)

    with _rate_limit_lock:
        bucket = _rate_limit_buckets.get(key)
        if bucket is None:
            bucket = deque()
            _rate_limit_buckets[key] = bucket

        while bucket and bucket[0] < cutoff:
            bucket.popleft()

        if len(bucket) >= limit:
            return False

        bucket.append(now)
        return True
