import os
import httpx
import time
from typing import List, Dict, Any

# simple in-memory cache for posts: {subreddit: (timestamp, data)}
_post_cache: Dict[str, tuple[float, List[Dict[str, Any]]]] = {}
CACHE_TTL = 600  # 10 minutes


async def fetch_reddit_posts(subreddit: str, limit: int = 100) -> List[Dict[str, Any]]:
    now = time.time()
    if subreddit in _post_cache:
        ts, data = _post_cache[subreddit]
        if now - ts < CACHE_TTL:
            return data

    base = "https://arctic-shift.photon-reddit.com"
    params = {"subreddit": subreddit, "limit": limit}
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(f"{base}/r/{subreddit}/new", params=params)
        resp.raise_for_status()
        posts = resp.json().get("data", [])
    # simple quality filtering might go here
    _post_cache[subreddit] = (now, posts)
    return posts


async def fetch_comments_for_post(post_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    """Fetch top-level comments for a given post ID from Arctic Shift mirror."""
    base = "https://arctic-shift.photon-reddit.com"
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(f"{base}/comments/{post_id}")
        resp.raise_for_status()
        data = resp.json().get("data", {})
    # the structure may be nested; attempt to gather some comments
    comments = []
    for item in data.get("children", []):
        body = item.get("body") or item.get("data", {}).get("body")
        if body:
            comments.append({"body": body, "id": item.get("id")})
            if len(comments) >= limit:
                break
    return comments


async def analyze_posts_with_ai(posts: List[Dict[str, Any]], comments: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Call OpenAI GPT-4o-mini to analyze sentiment/themes. Returns analysis dict."""
    import openai

    openai.api_key = os.getenv("OPENAI_API_KEY")
    if not openai.api_key:
        raise RuntimeError("OpenAI API key not configured")

    # build prompt as described in spec (simplified)
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
    # naive parse
    try:
        import json
        analysis = json.loads(text)
    except Exception:
        analysis = {"error": "failed to parse output", "raw": text}
    return analysis
