import math
import json
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import httpx

ARCTIC_SHIFT_BASE = "https://arctic-shift.photon-reddit.com"


POST_FIELDS = "id,title,selftext,created_utc,score,num_comments,author,subreddit"


COMMENT_FIELDS = "id,body,created_utc,score,author,parent_id"


CACHE_TTL = 600  # 10 minutes


DISCOVERY_CACHE_TTL = 24 * 60 * 60  # 24 hours


MULTI_SCAN_CACHE_TTL = 10 * 60  # 10 minutes


WINDOWS: List[Tuple[str, str]] = [("48h", "0h"), ("8d", "48h"), ("30d", "8d")]


MAX_POSTS_FINAL = 100


MAX_POSTS_PER_AUTHOR = 3


MAX_NO_COMMENT_POSTS = 20


MIN_RECENT_POSTS = 20


DISCOVERY_MAX_RESULTS = 10


DISCOVERY_MAX_CANDIDATES = 30


DISCOVERY_SAMPLE_POSTS = 25


DISCOVERY_OPENAI_TOP = 10


MAX_MULTI_SUBREDDITS = 5


BREAKDOWN_MAX_POSTS_PER_SUBREDDIT = 8


BREAKDOWN_SELFTEXT_TRUNCATE = 220


TOP_POSTS_FOR_COMMENTS = 15


MAX_COMMENTS_PER_POST = 10


COMMENT_BODY_TRUNCATE = 400


POST_SELFTEXT_TRUNCATE = 500


COMMENT_FETCH_DELAY = 0.2


_post_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}


_comments_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}


_discovery_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}


_multi_scan_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}


_subreddit_breakdown_cache: Dict[str, Tuple[float, Dict[str, Any]]] = {}


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


def _tokenize_text(value: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", (value or "").lower())


def _normalize_game_lookup_key(game_name: str) -> str:
    return " ".join(_tokenize_text(game_name))


def _build_subreddit_prefixes(game_name: str) -> List[str]:
    tokens = _tokenize_text(game_name)
    if not tokens:
        return []

    prefixes: List[str] = []

    def _add(term: str) -> None:
        cleaned = re.sub(r"[^a-z0-9_]", "", (term or "").lower())
        if len(cleaned) < 2:
            return
        if cleaned not in prefixes:
            prefixes.append(cleaned)

    _add("".join(tokens))
    _add("_".join(tokens))

    for token in tokens:
        _add(token)
        max_len = min(len(token), 8)
        for length in range(3, max_len + 1):
            _add(token[:length])

    for span in (2, 3):
        if len(tokens) >= span:
            _add("".join(tokens[:span]))
            _add("_".join(tokens[:span]))

    return prefixes[:20]


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


