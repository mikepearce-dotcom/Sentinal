import asyncio
import json
import math
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

ARCTIC_SHIFT_BASE = "https://arctic-shift.photon-reddit.com"

POST_FIELDS = "id,title,selftext,created_utc,score,num_comments,author,subreddit"
COMMENT_FIELDS = "id,body,created_utc,score,author,parent_id"

CACHE_TTL = 600  # 10 minutes
WINDOWS: List[Tuple[str, str]] = [("48h", "0h"), ("8d", "48h"), ("30d", "8d")]

MAX_POSTS_FINAL = 100
MAX_POSTS_PER_AUTHOR = 3
MAX_NO_COMMENT_POSTS = 20
MIN_RECENT_POSTS = 20

TOP_POSTS_FOR_COMMENTS = 15
MAX_COMMENTS_PER_POST = 10
COMMENT_BODY_TRUNCATE = 400
POST_SELFTEXT_TRUNCATE = 500
COMMENT_FETCH_DELAY = 0.2

_post_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
_comments_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}


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


def _extract_json_payload(text: str) -> Optional[Dict[str, Any]]:
    content = (text or "").strip()
    if not content:
        return None

    try:
        parsed = json.loads(content)
        if isinstance(parsed, dict):
            return parsed
    except Exception:
        pass

    fence_match = re.search(r"```(?:json)?\s*(.*?)\s*```", content, re.IGNORECASE | re.DOTALL)
    if fence_match:
        fenced_body = fence_match.group(1).strip()
        try:
            parsed = json.loads(fenced_body)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

    object_match = re.search(r"\{.*\}", content, re.DOTALL)
    if object_match:
        candidate = object_match.group(0)
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except Exception:
            pass

    return None


def _format_permalink(post_id: str) -> str:
    return f"https://www.reddit.com/comments/{post_id}/"


def _map_post(item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    post_id = item.get("id")
    if not post_id:
        return None

    return {
        "id": str(post_id),
        "title": item.get("title", "") or "",
        "selftext": item.get("selftext", "") or "",
        "created_utc": item.get("created_utc", 0) or 0,
        "score": item.get("score", 0) or 0,
        "num_comments": item.get("num_comments", 0) or 0,
        "author": item.get("author", "") or "",
        "subreddit": item.get("subreddit", "") or "",
        "permalink": _format_permalink(str(post_id)),
    }


async def _fetch_posts_window(normalized_subreddit: str, after: str, before: str) -> List[Dict[str, Any]]:
    params = {
        "subreddit": normalized_subreddit,
        "after": after,
        "before": before,
        "sort": "desc",
        "limit": 100,
        "fields": POST_FIELDS,
    }
    headers = {
        "User-Agent": "SentientTracker/1.0",
        "Accept": "application/json",
    }

    async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
        resp = await client.get(f"{ARCTIC_SHIFT_BASE}/api/posts/search", params=params, headers=headers)

    if resp.status_code == 404:
        return []

    if resp.status_code != 200:
        detail = _extract_error_detail(resp)
        suffix = f": {detail}" if detail else ""
        raise RuntimeError(f"Arctic Shift posts request failed (HTTP {resp.status_code}){suffix}")

    try:
        payload = resp.json()
    except Exception as exc:
        raise RuntimeError("Invalid JSON response from Arctic Shift posts API") from exc

    rows = payload.get("data", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        raise RuntimeError("Unexpected Arctic Shift posts response format")

    mapped: List[Dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue
        post = _map_post(item)
        if post is not None:
            mapped.append(post)

    return mapped


def _apply_quality_filter(posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []

    for post in posts:
        num_comments = int(post.get("num_comments", 0) or 0)
        score = int(post.get("score", 0) or 0)
        selftext = str(post.get("selftext", "") or "")
        title = str(post.get("title", "") or "")

        is_low_quality = (
            num_comments == 0
            and score <= 1
            and (len(selftext) == 0 or len(selftext) < 80)
            and len(title) < 25
        )

        if not is_low_quality:
            filtered.append(post)

    return filtered


def _calculate_post_rank(post: Dict[str, Any]) -> float:
    score = max(0, int(post.get("score", 0) or 0))
    num_comments = max(0, int(post.get("num_comments", 0) or 0))
    selftext = str(post.get("selftext", "") or "")

    engagement = math.log(score + 1) + 2 * math.log(num_comments + 1)
    text_bonus = min(len(selftext) / 500.0, 1.0)
    rank = engagement + 0.35 * text_bonus

    # Small penalty for very low-signal posts with no discussion depth.
    if num_comments == 0 and len(selftext) < 100:
        rank -= 0.25

    return rank


def _apply_diversity_and_recency(posts: List[Dict[str, Any]], max_posts: int) -> List[Dict[str, Any]]:
    if not posts:
        return []

    sorted_posts = sorted(posts, key=_calculate_post_rank, reverse=True)

    author_count: Dict[str, int] = {}
    zero_comment_count = 0

    now = time.time()
    three_days_ago = now - (3 * 24 * 60 * 60)
    recent_target = min(MIN_RECENT_POSTS, max_posts)

    selected: List[Dict[str, Any]] = []
    deferred_recent: List[Dict[str, Any]] = []
    recent_count = 0

    for post in sorted_posts:
        if len(selected) >= max_posts:
            break

        author = str(post.get("author", "unknown") or "unknown")
        num_comments = int(post.get("num_comments", 0) or 0)
        created_utc = float(post.get("created_utc", 0) or 0)
        is_recent = created_utc > three_days_ago

        if author_count.get(author, 0) >= MAX_POSTS_PER_AUTHOR:
            if is_recent:
                deferred_recent.append(post)
            continue

        if num_comments == 0 and zero_comment_count >= MAX_NO_COMMENT_POSTS:
            if is_recent:
                deferred_recent.append(post)
            continue

        selected.append(post)
        author_count[author] = author_count.get(author, 0) + 1

        if num_comments == 0:
            zero_comment_count += 1
        if is_recent:
            recent_count += 1

    if recent_count < recent_target and deferred_recent:
        deferred_recent.sort(key=lambda p: float(p.get("created_utc", 0) or 0), reverse=True)

        for post in deferred_recent:
            if recent_count >= recent_target:
                break

            if len(selected) < max_posts:
                selected.append(post)
                recent_count += 1
                continue

            replace_index = -1
            for idx in range(len(selected) - 1, -1, -1):
                existing = selected[idx]
                existing_recent = float(existing.get("created_utc", 0) or 0) > three_days_ago
                if not existing_recent:
                    replace_index = idx
                    break

            if replace_index >= 0:
                selected[replace_index] = post
                recent_count += 1

    return selected[:max_posts]


def _clean_comment_body(body: str) -> str:
    clean = (body or "").strip()
    clean = re.sub(r"/?u/[A-Za-z0-9_-]+", "[user]", clean)
    if len(clean) > COMMENT_BODY_TRUNCATE:
        clean = clean[:COMMENT_BODY_TRUNCATE] + "..."
    return clean


def _select_best_comments(comments: List[Dict[str, Any]], max_count: int) -> List[Dict[str, Any]]:
    sorted_comments = sorted(
        comments,
        key=lambda c: (
            int(c.get("score", 0) or 0),
            len(str(c.get("body", "") or "")),
            float(c.get("created_utc", 0) or 0),
        ),
        reverse=True,
    )

    selected: List[Dict[str, Any]] = []
    author_counts: Dict[str, int] = {}

    for item in sorted_comments:
        if len(selected) >= max_count:
            break

        author = str(item.get("author", "") or "").lower()
        if author and author_counts.get(author, 0) >= 2:
            continue

        selected.append(
            {
                "id": str(item.get("id") or ""),
                "body": _clean_comment_body(str(item.get("body", "") or "")),
                "score": int(item.get("score", 0) or 0),
                "created_utc": int(item.get("created_utc", 0) or 0),
                "author": str(item.get("author", "") or ""),
            }
        )

        if author:
            author_counts[author] = author_counts.get(author, 0) + 1

    return selected


async def fetch_reddit_posts(subreddit: str, limit: int = 100) -> List[Dict[str, Any]]:
    normalized = _normalize_subreddit(subreddit)
    if not normalized:
        return []

    now = time.time()
    cached = _post_cache.get(normalized)
    if cached and now - cached[0] < CACHE_TTL:
        return cached[1]

    target_limit = min(max(limit, 1), MAX_POSTS_FINAL)
    merged_by_id: Dict[str, Dict[str, Any]] = {}
    last_error: Optional[Exception] = None

    for after, before in WINDOWS:
        try:
            window_posts = await _fetch_posts_window(normalized, after=after, before=before)
        except Exception as exc:
            last_error = exc
            continue

        for post in window_posts:
            merged_by_id[post["id"]] = post

        # Once we have enough candidates for quality + diversity, stop requesting more windows.
        if len(merged_by_id) >= target_limit * 2:
            break

    if not merged_by_id:
        if last_error is not None:
            raise RuntimeError(str(last_error))
        _post_cache[normalized] = (now, [])
        return []

    candidates = list(merged_by_id.values())
    quality_filtered = _apply_quality_filter(candidates)
    high_signal = quality_filtered if quality_filtered else candidates

    final_posts = _apply_diversity_and_recency(high_signal, max_posts=target_limit)
    _post_cache[normalized] = (now, final_posts)
    return final_posts


async def fetch_comments_for_post(post_id: str, limit: int = 50) -> List[Dict[str, Any]]:
    if not post_id:
        return []

    now = time.time()
    cached = _comments_cache.get(post_id)
    if cached and now - cached[0] < CACHE_TTL:
        cached_comments = cached[1]
        return _select_best_comments(cached_comments, max_count=min(max(limit, 1), 100))

    params = {
        "link_id": f"t3_{post_id}",
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

        parent_id = str(item.get("parent_id", "") or "")
        if parent_id and not parent_id.startswith("t3_"):
            continue

        body = str(item.get("body", "") or "").strip()
        if not body or body in ("[deleted]", "[removed]"):
            continue

        comments.append(
            {
                "id": str(item.get("id") or ""),
                "body": body,
                "score": int(item.get("score", 0) or 0),
                "created_utc": int(item.get("created_utc", 0) or 0),
                "author": str(item.get("author", "") or ""),
            }
        )

    _comments_cache[post_id] = (now, comments)
    return _select_best_comments(comments, max_count=min(max(limit, 1), 100))


async def sample_comments_for_posts(
    posts: List[Dict[str, Any]],
    max_posts: int = TOP_POSTS_FOR_COMMENTS,
    max_comments_per_post: int = MAX_COMMENTS_PER_POST,
) -> List[Dict[str, Any]]:
    if not posts:
        return []

    ranked_posts = sorted(posts, key=_calculate_post_rank, reverse=True)[: max(max_posts, 1)]
    sampled: List[Dict[str, Any]] = []

    for post in ranked_posts:
        post_id = str(post.get("id") or "")
        if not post_id:
            continue

        try:
            comments = await fetch_comments_for_post(post_id, limit=max(max_comments_per_post, 1))
        except Exception:
            comments = []

        for comment in comments:
            item = dict(comment)
            item["source_post_id"] = post_id
            sampled.append(item)

        await asyncio.sleep(COMMENT_FETCH_DELAY)

    return sampled


def _build_analysis_prompt(
    posts: List[Dict[str, Any]],
    comments: List[Dict[str, Any]],
    game_name: str,
    keywords: str,
) -> str:
    post_summaries: List[str] = []
    for post in posts[:MAX_POSTS_FINAL]:
        post_id = str(post.get("id") or "")
        title = str(post.get("title", "") or "")
        score = int(post.get("score", 0) or 0)
        num_comments = int(post.get("num_comments", 0) or 0)
        selftext = str(post.get("selftext", "") or "")[:POST_SELFTEXT_TRUNCATE]

        line = f"[POST:{post_id}] [{score} pts, {num_comments} comments] {title}"
        if selftext and selftext not in ("[removed]", "[deleted]"):
            line += f"\n  Content: {selftext.replace(chr(10), ' ').strip()}"

        post_summaries.append(line)

    comments_text = ""
    if comments:
        comment_lines: List[str] = ["COMMENT SAMPLES FROM TOP POSTS:"]
        for comment in comments[: TOP_POSTS_FOR_COMMENTS * MAX_COMMENTS_PER_POST]:
            source_post = str(comment.get("source_post_id") or "")
            body = str(comment.get("body", "") or "")
            score = int(comment.get("score", 0) or 0)
            comment_lines.append(f"- [POST:{source_post}] [{score} pts] {body}")
        comments_text = "\n".join(comment_lines)

    subreddit_name = "Unknown"
    for post in posts:
        value = str(post.get("subreddit", "") or "").strip()
        if value:
            subreddit_name = value if value.lower().startswith("r/") else f"r/{value}"
            break

    now_ts = time.time()
    recent_cutoff = now_ts - (3 * 24 * 60 * 60)
    recent_posts = sum(1 for p in posts if float(p.get("created_utc", 0) or 0) >= recent_cutoff)
    older_posts = max(0, len(posts) - recent_posts)

    keyword_note = f"\nKeywords to watch for: {keywords}" if keywords else ""

    return f"""Analyze these {len(posts)} Reddit posts and {len(comments)} top comment samples about the game "{game_name or 'Unknown Game'}".

IMPORTANT INSTRUCTIONS:
- Do NOT assume PvP, PvE, modes, platforms, or monetisation unless directly stated in the posts/comments.
- Ignore toxic language and personal attacks. Summarize professionally.
- If a keyword list is provided, prioritize those topics in themes and sentiment context.

SENTIMENT SUMMARY REQUIREMENTS (2-3 sentences):
- Sentence 1: overall sentiment and the primary driver.
- Sentence 2: top two concrete pain points.
- Sentence 3 (optional): strongest positive or retention driver.
- Include at least two [POST:post_id] references inside sentiment_summary.

THEMES REQUIREMENTS:
- Return 5-10 themes as strings.
- Format each as: "Theme name - specific explanation grounded in player feedback".
- At least half of the themes must include a [POST:post_id] reference.
- Themes must be specific and actionable (no one-word generic labels).

PAIN POINTS / WINS REQUIREMENTS:
- Keep the same structure with "text" and "evidence" fields only.
- Each text must describe a repeat issue or repeat strength, not a one-off complaint/praise.
- Evidence links must use this format: https://www.reddit.com/comments/POST_ID/
- Use evidence from different posts where possible.

REQUIRED JSON OUTPUT:
1. sentiment_label: "Positive", "Mixed", or "Negative"
2. sentiment_summary: 2-3 sentences with required structure
3. themes: array of 5-10 specific strings
4. pain_points: array of exactly 5 objects with:
   - text: string
   - evidence: array of 1-2 Reddit links
5. wins: array of exactly 5 objects with:
   - text: string
   - evidence: array of 1-2 Reddit links
{keyword_note}

SCAN CONTEXT:
- Subreddit: {subreddit_name}
- Posts analyzed: {len(posts)}
- Comments sampled: {len(comments)}
- Time coverage: {recent_posts} recent posts (last 3 days), {older_posts} older posts

POSTS:
{chr(10).join(post_summaries)}

{comments_text}

Respond with valid JSON only, no markdown fences.
"""


def _normalize_sentiment_label(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if "positive" in raw:
        return "Positive"
    if "negative" in raw:
        return "Negative"
    if "mixed" in raw:
        return "Mixed"
    return "Unknown"


def _normalize_evidence_links(value: Any) -> List[str]:
    if isinstance(value, list):
        links = [str(v).strip() for v in value if str(v).strip()]
    elif isinstance(value, str):
        links = [value.strip()] if value.strip() else []
    else:
        links = []

    normalized: List[str] = []
    for link in links:
        if "reddit.com/comments/" in link:
            cleaned = link.split()[0]
            if cleaned not in normalized:
                normalized.append(cleaned)

    return normalized[:2]


def _normalize_insight_items(value: Any) -> List[Dict[str, Any]]:
    if not isinstance(value, list):
        return []

    items: List[Dict[str, Any]] = []
    for raw_item in value:
        if isinstance(raw_item, str):
            text = raw_item.strip()
            if text:
                items.append({"text": text, "evidence": []})
            continue

        if isinstance(raw_item, dict):
            text = str(
                raw_item.get("text")
                or raw_item.get("summary")
                or raw_item.get("point")
                or raw_item.get("title")
                or ""
            ).strip()
            if not text:
                continue

            evidence = _normalize_evidence_links(raw_item.get("evidence"))
            items.append({"text": text, "evidence": evidence})

    return items[:5]


def _normalize_themes(value: Any) -> List[str]:
    if not isinstance(value, list):
        return []

    themes: List[str] = []
    for item in value:
        text = str(item).strip()
        if text and text not in themes:
            themes.append(text)
        if len(themes) >= 10:
            break

    return themes


def _normalize_analysis(result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "sentiment_label": _normalize_sentiment_label(result.get("sentiment_label")),
        "sentiment_summary": str(result.get("sentiment_summary", "") or "").strip(),
        "themes": _normalize_themes(result.get("themes")),
        "pain_points": _normalize_insight_items(result.get("pain_points")),
        "wins": _normalize_insight_items(result.get("wins")),
    }


async def analyze_posts_with_ai(
    posts: List[Dict[str, Any]],
    comments: List[Dict[str, Any]],
    game_name: str = "",
    keywords: str = "",
) -> Dict[str, Any]:
    """Analyze Reddit posts/comments with OpenAI and return normalized sentiment output."""
    import openai

    openai.api_key = os.getenv("OPENAI_API_KEY")
    if not openai.api_key:
        raise RuntimeError("OpenAI API key not configured")

    prompt = _build_analysis_prompt(posts, comments, game_name=game_name, keywords=keywords)

    response = await openai.ChatCompletion.acreate(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": (
                    "You are an expert gaming community analyst. "
                    "Return valid JSON only and avoid quoting toxic content directly."
                ),
            },
            {"role": "user", "content": prompt},
        ],
        temperature=0.2,
        max_tokens=1800,
    )

    text = response.choices[0].message.content or ""
    parsed = _extract_json_payload(text)
    if parsed is None:
        return {"error": "failed to parse output", "raw": text}

    return _normalize_analysis(parsed)

