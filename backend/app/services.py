import os
import re
import time
from typing import Any, Dict, List, Tuple

import httpx

ARCTIC_SHIFT_BASE = "https://arctic-shift.photon-reddit.com"
# Use only documented selectable fields.
POST_FIELDS = "id,title,selftext,created_utc,score,num_comments,author"
COMMENT_FIELDS = "id,body,created_utc,score,author,parent_id"

# simple in-memory caches
_post_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
_comments_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
CACHE_TTL = 600  # 10 minutes


def _normalize_subreddit(value: str) -> str:
    if not value:
        return ""

    raw = value.strip().strip("/")
    if raw.lower().startswith("r/"):
        raw = raw[2:]

    match = re.search(r"reddit\.com/r/([^/?#]+)", raw, re.IGNORECASE)
    if match:
        raw = match.group(1)

    return raw.strip().strip("/")


def _extract_error_detail(resp: httpx.Response) -> str:
    try:
        payload = resp.json()
        if isinstance(payload, dict):
            for key in ("error", "message", "detail"):
                value = payload.get(key)
                if isinstance(value, str) and value.strip():
                    return value.strip()
            return str(payload)
        return str(payload)
    except Exception:
        text = (resp.text or "").strip()
        return text[:300] if text else ""


async def fetch_reddit_posts(subreddit: str, limit: int = 100) -> List[Dict[str, Any]]:
    normalized = _normalize_subreddit(subreddit)
    if not normalized:
        return []

    now = time.time()
    cached = _post_cache.get(normalized)
    if cached and now - cached[0] < CACHE_TTL:
        return cached[1]

    params = {
        "subreddit": normalized,
        "sort": "desc",
        "limit": min(max(limit, 1), 100),
        "fields": POST_FIELDS,
    }
    headers = {
        "User-Agent": "SentientTracker/1.0",
        "Accept": "application/json",
    }

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        resp = await client.get(f"{ARCTIC_SHIFT_BASE}/api/posts/search", params=params, headers=headers)

    if resp.status_code == 404:
        _post_cache[normalized] = (now, [])
        return []
    if resp.status_code != 200:
        detail = _extract_error_detail(resp)
        suffix = f": {detail}" if detail else ""
        raise RuntimeError(f"Arctic Shift posts request failed (HTTP {resp.status_code}){suffix}")

    try:
        data = resp.json()
    except Exception as exc:
        raise RuntimeError("Invalid JSON response from Arctic Shift posts API") from exc

    raw_posts = data.get("data", []) if isinstance(data, dict) else []
    if not isinstance(raw_posts, list):
        raise RuntimeError("Unexpected Arctic Shift posts response format")

    posts: List[Dict[str, Any]] = []
    for item in raw_posts:
        if not isinstance(item, dict):
            continue

        post_id = item.get("id")
        if not post_id:
            continue

        posts.append(
            {
                "id": str(post_id),
                "title": item.get("title", "") or "",
                "selftext": item.get("selftext", "") or "",
                "created_utc": item.get("created_utc", 0) or 0,
                "score": item.get("score", 0) or 0,
                "num_comments": item.get("num_comments", 0) or 0,
                "author": item.get("author", "") or "",
                "permalink": f"https://www.reddit.com/comments/{post_id}/",
            }
        )

    _post_cache[normalized] = (now, posts)
    return posts


async def fetch_comments_for_post(post_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    if not post_id:
        return []

    now = time.time()
    cached = _comments_cache.get(post_id)
    if cached and now - cached[0] < CACHE_TTL:
        return cached[1][:limit]

    params = {
        # Arctic Shift accepts t3_ prefix, but bare ID is documented and safer.
        "link_id": post_id,
        "sort": "desc",
        "limit": 100,
        "fields": COMMENT_FIELDS,
    }
    headers = {
        "User-Agent": "SentientTracker/1.0",
        "Accept": "application/json",
    }

    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        resp = await client.get(f"{ARCTIC_SHIFT_BASE}/api/comments/search", params=params, headers=headers)

    if resp.status_code in (404, 400):
        _comments_cache[post_id] = (now, [])
        return []
    if resp.status_code != 200:
        detail = _extract_error_detail(resp)
        suffix = f": {detail}" if detail else ""
        raise RuntimeError(f"Arctic Shift comments request failed (HTTP {resp.status_code}){suffix}")

    try:
        data = resp.json()
    except Exception as exc:
        raise RuntimeError("Invalid JSON response from Arctic Shift comments API") from exc

    raw_comments = data.get("data", []) if isinstance(data, dict) else []
    if not isinstance(raw_comments, list):
        raise RuntimeError("Unexpected Arctic Shift comments response format")

    comments: List[Dict[str, Any]] = []
    for item in raw_comments:
        if not isinstance(item, dict):
            continue

        parent_id = str(item.get("parent_id", ""))
        if parent_id and not parent_id.startswith("t3_"):
            continue

        body = (item.get("body") or "").strip()
        if not body or body in ("[deleted]", "[removed]"):
            continue

        comments.append(
            {
                "id": str(item.get("id") or ""),
                "body": body,
                "score": item.get("score", 0) or 0,
                "created_utc": item.get("created_utc", 0) or 0,
                "author": item.get("author", "") or "",
            }
        )

        if len(comments) >= limit:
            break

    _comments_cache[post_id] = (now, comments)
    return comments


async def analyze_posts_with_ai(posts: List[Dict[str, Any]], comments: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Call OpenAI GPT-4o-mini to analyze sentiment/themes. Returns analysis dict."""
    import openai

    openai.api_key = os.getenv("OPENAI_API_KEY")
    if not openai.api_key:
        raise RuntimeError("OpenAI API key not configured")

    prompt = "You are an analyst. "
    prompt += "Analyze these posts and comments. "
    prompt += "Provide sentiment_label, sentiment_summary, themes, pain_points, wins in JSON.\n"
    prompt += "Posts:\n"
    for p in posts:
        prompt += f"- {p.get('title', '')}\n"
    prompt += "Comments:\n"
    for c in comments[:20]:
        prompt += f"- {c.get('body', '')}\n"
    prompt += "\nOutput JSON only."

    response = await openai.ChatCompletion.acreate(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1000,
    )

    text = response.choices[0].message.content
    try:
        import json

        analysis = json.loads(text)
    except Exception:
        analysis = {"error": "failed to parse output", "raw": text}
    return analysis
