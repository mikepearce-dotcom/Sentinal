import asyncio
from collections import Counter
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


async def _search_subreddits_by_prefix(prefix: str, limit: int = 25) -> List[Dict[str, Any]]:
    clean_prefix = re.sub(r"[^a-z0-9_]", "", (prefix or "").lower())
    if len(clean_prefix) < 2:
        return []

    params = {
        "subreddit_prefix": clean_prefix,
        "limit": max(1, min(limit, 1000)),
    }
    headers = {
        "User-Agent": "SentientTracker/1.0",
        "Accept": "application/json",
    }

    async with httpx.AsyncClient(timeout=20.0, follow_redirects=True) as client:
        resp = await client.get(f"{ARCTIC_SHIFT_BASE}/api/subreddits/search", params=params, headers=headers)

    if resp.status_code in (400, 404):
        return []

    if resp.status_code != 200:
        detail = _extract_error_detail(resp)
        suffix = f": {detail}" if detail else ""
        raise RuntimeError(f"Arctic Shift subreddit search failed (HTTP {resp.status_code}){suffix}")

    try:
        payload = resp.json()
    except Exception as exc:
        raise RuntimeError("Invalid JSON response from Arctic Shift subreddit search") from exc

    rows = payload.get("data", []) if isinstance(payload, dict) else []
    if not isinstance(rows, list):
        return []

    candidates: List[Dict[str, Any]] = []
    for item in rows:
        if not isinstance(item, dict):
            continue

        subreddit = _normalize_subreddit(
            str(item.get("subreddit") or item.get("display_name") or item.get("name") or "")
        )
        if not subreddit:
            continue

        subscribers_raw = item.get("subscribers", 0)
        try:
            subscribers = int(subscribers_raw or 0)
        except Exception:
            subscribers = 0

        title = str(item.get("title", "") or "")
        description = str(item.get("public_description") or item.get("description") or "")

        candidates.append(
            {
                "subreddit": subreddit,
                "subscribers": subscribers,
                "title": title,
                "description": description,
            }
        )

    return candidates


def _extract_signal_tokens(game_name: str) -> List[str]:
    tokens = _tokenize_text(game_name)
    if not tokens:
        return []

    signal_tokens = [t for t in tokens if len(t) >= 3]
    return signal_tokens or tokens


def _name_similarity_score(game_tokens: List[str], candidate: Dict[str, Any]) -> float:
    if not game_tokens:
        return 0.0

    candidate_blob = " ".join(
        [
            str(candidate.get("subreddit", "") or ""),
            str(candidate.get("title", "") or ""),
            str(candidate.get("description", "") or ""),
        ]
    )
    candidate_tokens = set(_tokenize_text(candidate_blob))
    if not candidate_tokens:
        return 0.0

    game_token_set = set(game_tokens)
    overlap = len(game_token_set.intersection(candidate_tokens))
    return overlap / float(len(game_token_set))


def _content_relevance_score(game_tokens: List[str], posts: List[Dict[str, Any]]) -> float:
    if not game_tokens or not posts:
        return 0.0

    game_token_set = set(game_tokens)
    matches = 0
    for post in posts:
        title_tokens = set(_tokenize_text(str(post.get("title", "") or "")))
        if title_tokens.intersection(game_token_set):
            matches += 1

    return matches / float(len(posts))


def _build_discovery_reason(content_score: float, activity_score: float, name_score: float) -> str:
    if content_score >= 0.5 and activity_score >= 1.5:
        return "High content relevance and consistent discussion"
    if name_score >= 0.5 and activity_score >= 1.0:
        return "Name match with active engagement"
    if content_score >= 0.35:
        return "Frequent game mentions in recent posts"
    if activity_score >= 2.0:
        return "Active subreddit with ongoing discussions"
    if name_score >= 0.35:
        return "Strong name similarity to the game title"
    return "Potential match based on available subreddit signals"


async def _openai_rerank_subreddit_candidates(
    game_name: str,
    candidates: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or not candidates:
        return []

    try:
        import openai

        openai.api_key = api_key

        lines: List[str] = []
        for index, candidate in enumerate(candidates, start=1):
            subreddit = str(candidate.get("subreddit", "") or "")
            subscribers = int(candidate.get("subscribers", 0) or 0)
            sample_titles = candidate.get("_sample_titles") or []
            if not isinstance(sample_titles, list):
                sample_titles = []

            lines.append(f"{index}. r/{subreddit} | subscribers={subscribers}")
            for title in sample_titles[:3]:
                lines.append(f"   - {str(title)}")

        prompt = (
            "You are selecting the best Reddit communities to scan for game feedback.\n"
            f"Game name: {game_name}\n\n"
            "Candidates:\n"
            + "\n".join(lines)
            + "\n\n"
            "Return strict JSON in this shape only:\n"
            '{"picks":[{"subreddit":"name","confidence":"High|Medium|Low","justification":"short reason"}]}'
            "\nChoose 3 to 5 subreddits. Prefer communities that are clearly about the game and have current discussion signal."
        )

        response = await openai.ChatCompletion.acreate(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "Return valid JSON only."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.2,
            max_tokens=500,
        )
        text = response.choices[0].message.content or ""
        parsed = _extract_json_payload(text)
        if not parsed:
            return []

        picks = parsed.get("picks")
        if not isinstance(picks, list):
            return []

        normalized: List[Dict[str, Any]] = []
        for item in picks:
            if not isinstance(item, dict):
                continue
            subreddit = _normalize_subreddit(str(item.get("subreddit", "") or ""))
            if not subreddit:
                continue
            normalized.append(
                {
                    "subreddit": subreddit,
                    "confidence": str(item.get("confidence", "") or "").strip(),
                    "justification": str(
                        item.get("justification") or item.get("reason") or ""
                    ).strip(),
                }
            )

        return normalized
    except Exception as exc:
        print(f"OpenAI subreddit rerank failed: {exc}")
        return []


def _apply_openai_rerank(
    candidates: List[Dict[str, Any]],
    picks: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not candidates or not picks:
        return candidates

    by_subreddit = {str(c.get("subreddit", "")).lower(): dict(c) for c in candidates}
    used = set()
    ranked: List[Dict[str, Any]] = []

    for rank, pick in enumerate(picks):
        subreddit = _normalize_subreddit(str(pick.get("subreddit", "") or "")).lower()
        if not subreddit or subreddit in used:
            continue

        candidate = by_subreddit.get(subreddit)
        if not candidate:
            continue

        confidence = str(pick.get("confidence", "") or "").strip().lower()
        justification = str(pick.get("justification", "") or "").strip()

        bonus = 0.0
        if confidence.startswith("high"):
            bonus = 0.25
        elif confidence.startswith("medium"):
            bonus = 0.15
        elif confidence.startswith("low"):
            bonus = 0.05

        candidate["score"] = round(float(candidate.get("score", 0.0)) + bonus, 4)
        if justification:
            suffix = "AI rerank"
            if confidence:
                suffix += f" ({confidence})"
            candidate["reason"] = f"{candidate.get('reason', '')}; {suffix}: {justification}".strip("; ")

        candidate["_ai_rank"] = rank
        ranked.append(candidate)
        used.add(subreddit)

    for candidate in candidates:
        sub = str(candidate.get("subreddit", "")).lower()
        if sub in used:
            continue
        remaining = dict(candidate)
        remaining["_ai_rank"] = 999
        ranked.append(remaining)

    ranked.sort(
        key=lambda item: (
            int(item.get("_ai_rank", 999)),
            -float(item.get("score", 0.0)),
            -int(item.get("subscribers", 0) or 0),
        )
    )

    cleaned: List[Dict[str, Any]] = []
    for item in ranked:
        row = dict(item)
        row.pop("_ai_rank", None)
        cleaned.append(row)
    return cleaned


async def discover_subreddits_for_game(
    game_name: str,
    max_results: int = 5,
) -> List[Dict[str, Any]]:
    lookup_key = _normalize_game_lookup_key(game_name)
    if not lookup_key:
        return []

    safe_max = max(1, min(max_results, DISCOVERY_MAX_RESULTS))

    now = time.time()
    cached = _discovery_cache.get(lookup_key)
    if cached and now - cached[0] < DISCOVERY_CACHE_TTL:
        return [dict(item) for item in cached[1][:safe_max]]

    prefixes = _build_subreddit_prefixes(game_name)
    if not prefixes:
        _discovery_cache[lookup_key] = (now, [])
        return []

    candidate_map: Dict[str, Dict[str, Any]] = {}
    for prefix in prefixes:
        try:
            candidates = await _search_subreddits_by_prefix(prefix, limit=25)
        except Exception as exc:
            print(f"Subreddit discovery prefix failed ({prefix}): {exc}")
            continue

        for candidate in candidates:
            subreddit = str(candidate.get("subreddit", "") or "").lower()
            if not subreddit:
                continue

            existing = candidate_map.get(subreddit)
            if existing is None:
                candidate_map[subreddit] = candidate
                continue

            if int(candidate.get("subscribers", 0) or 0) > int(existing.get("subscribers", 0) or 0):
                candidate_map[subreddit] = candidate

    if not candidate_map:
        _discovery_cache[lookup_key] = (now, [])
        return []

    ranked_candidates = sorted(
        candidate_map.values(),
        key=lambda item: int(item.get("subscribers", 0) or 0),
        reverse=True,
    )[:DISCOVERY_MAX_CANDIDATES]

    game_tokens = _extract_signal_tokens(game_name)
    scored: List[Dict[str, Any]] = []

    for candidate in ranked_candidates:
        subreddit = str(candidate.get("subreddit", "") or "")
        if not subreddit:
            continue

        try:
            sampled_posts = await _fetch_posts_window(subreddit, after="30d", before="14d")
        except Exception as exc:
            print(f"Subreddit sample fetch failed ({subreddit}): {exc}")
            sampled_posts = []

        sampled_posts = sampled_posts[:DISCOVERY_SAMPLE_POSTS]
        total_comments = sum(int(post.get("num_comments", 0) or 0) for post in sampled_posts)
        total_score = sum(int(post.get("score", 0) or 0) for post in sampled_posts)

        activity_score = math.log(1 + total_comments) + 0.5 * math.log(1 + total_score)
        name_score = _name_similarity_score(game_tokens, candidate)
        content_score = _content_relevance_score(game_tokens, sampled_posts)

        combined_score = (0.45 * content_score) + (0.35 * activity_score) + (0.20 * name_score)
        reason = _build_discovery_reason(content_score, activity_score, name_score)

        scored.append(
            {
                "subreddit": subreddit,
                "subscribers": int(candidate.get("subscribers", 0) or 0),
                "score": round(combined_score, 4),
                "reason": reason,
                "_sample_titles": [str(post.get("title", "") or "") for post in sampled_posts[:3]],
            }
        )

    if not scored:
        _discovery_cache[lookup_key] = (now, [])
        return []

    deterministic = sorted(
        scored,
        key=lambda item: (float(item.get("score", 0.0)), int(item.get("subscribers", 0) or 0)),
        reverse=True,
    )

    rerank_candidates = deterministic[:DISCOVERY_OPENAI_TOP]
    picks = await _openai_rerank_subreddit_candidates(game_name, rerank_candidates)
    reranked = _apply_openai_rerank(deterministic, picks) if picks else deterministic

    cached_rows: List[Dict[str, Any]] = []
    for item in reranked[:DISCOVERY_MAX_RESULTS]:
        cached_rows.append(
            {
                "subreddit": str(item.get("subreddit", "") or ""),
                "subscribers": int(item.get("subscribers", 0) or 0),
                "score": float(item.get("score", 0.0) or 0.0),
                "reason": str(item.get("reason", "") or ""),
            }
        )

    _discovery_cache[lookup_key] = (now, cached_rows)
    return [dict(item) for item in cached_rows[:safe_max]]


async def fetch_posts_for_subreddits(
    subreddits: List[str],
    per_sub_limit: int = 80,
    total_limit: int = 150,
) -> List[Dict[str, Any]]:
    if not subreddits:
        return []

    unique_subreddits: List[str] = []
    seen = set()
    for raw in subreddits:
        normalized = _normalize_subreddit(str(raw or ""))
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        unique_subreddits.append(normalized)

    if not unique_subreddits:
        return []

    safe_per_sub_limit = max(1, min(per_sub_limit, MAX_POSTS_FINAL))
    safe_total_limit = max(1, min(total_limit, 500))

    tasks = [fetch_reddit_posts(subreddit, limit=safe_per_sub_limit) for subreddit in unique_subreddits]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    merged_by_id: Dict[str, Dict[str, Any]] = {}
    for result in results:
        if isinstance(result, Exception):
            continue

        for post in result:
            post_id = str(post.get("id") or "")
            if not post_id:
                continue

            existing = merged_by_id.get(post_id)
            if existing is None:
                merged_by_id[post_id] = post
                continue

            if _calculate_post_rank(post) > _calculate_post_rank(existing):
                merged_by_id[post_id] = post

    if not merged_by_id:
        return []

    ranked_posts = sorted(merged_by_id.values(), key=_calculate_post_rank, reverse=True)
    return ranked_posts[:safe_total_limit]


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
- Themes must include concrete subtopic + cause/effect detail (avoid broad labels like "Gameplay Mechanics").
- Themes must be specific and actionable (no one-word generic labels).

PAIN POINTS / WINS REQUIREMENTS:
- Keep the same structure with "text" and "evidence" fields only.
- Each text must describe a repeat issue or repeat strength, not a one-off complaint/praise.
- Focus on product/game feedback, not vague community activity fluff.
- Evidence links must use this format: https://www.reddit.com/comments/POST_ID/
- Evidence must be full links only; never placeholders like [source 1] and never [POST:post_id] in evidence arrays.
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
    raw_values: List[str] = []
    if isinstance(value, list):
        raw_values = [str(v).strip() for v in value if str(v).strip()]
    elif isinstance(value, str):
        raw_values = [value.strip()] if value.strip() else []

    normalized: List[str] = []
    for raw in raw_values:
        candidate = str(raw or "").strip()
        if not candidate:
            continue

        match = re.search(r"reddit\.com/comments/([a-z0-9_]+)", candidate, re.IGNORECASE)
        if match:
            canonical = _format_permalink(match.group(1))
            if canonical not in normalized:
                normalized.append(canonical)
            continue

        if re.fullmatch(r"[a-z0-9_]{5,}", candidate, re.IGNORECASE):
            canonical = _format_permalink(candidate)
            if canonical not in normalized:
                normalized.append(canonical)

    return normalized[:2]


def _normalize_insight_items(value: Any) -> List[Dict[str, Any]]:
    if not isinstance(value, list):
        return []

    items: List[Dict[str, Any]] = []
    for raw_item in value:
        text_value = ""
        evidence: List[str] = []
        candidate_post_ids: List[str] = []

        if isinstance(raw_item, str):
            text_value = raw_item.strip()
        elif isinstance(raw_item, dict):
            text_value = str(
                raw_item.get("text")
                or raw_item.get("summary")
                or raw_item.get("point")
                or raw_item.get("title")
                or ""
            ).strip()
            evidence = _normalize_evidence_links(raw_item.get("evidence"))

            for key in ("post_id", "source_post_id", "id"):
                raw_id = str(raw_item.get(key) or "").strip()
                if raw_id and raw_id not in candidate_post_ids:
                    candidate_post_ids.append(raw_id)

        if not text_value:
            continue

        for post_id in _extract_post_ids(text_value):
            if post_id not in candidate_post_ids:
                candidate_post_ids.append(post_id)

        if not evidence and candidate_post_ids:
            for post_id in candidate_post_ids:
                link = _format_permalink(post_id)
                if link not in evidence:
                    evidence.append(link)
                if len(evidence) >= 2:
                    break

        items.append({"text": text_value, "evidence": evidence[:2]})

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


def _post_signal_blob(post: Dict[str, Any]) -> str:
    return f"{post.get('title', '')} {post.get('selftext', '')}".lower()


def _post_engagement_weight(post: Dict[str, Any]) -> float:
    score = max(0, int(post.get("score", 0) or 0))
    comments = max(0, int(post.get("num_comments", 0) or 0))
    return 1.0 + math.log(score + 1) + 1.2 * math.log(comments + 1)


NEGATIVE_SIGNAL_TERMS = {
    "bug", "broken", "issue", "issues", "crash", "crashes", "lag", "stutter", "cheater", "cheaters",
    "queue", "matchmaking", "delay", "disconnect", "exploit", "unbalanced", "frustrating", "refund",
    "paywall", "grind", "toxic", "nerf",
}

POSITIVE_SIGNAL_TERMS = {
    "fun", "great", "good", "love", "enjoy", "smooth", "awesome", "improved", "improvement",
    "best", "better", "satisfying", "hype", "rewarding", "polished", "addictive", "fair",
}

THEME_STOP_WORDS = {
    "the", "and", "with", "from", "this", "that", "have", "your", "about", "into", "they", "their",
    "them", "what", "when", "where", "which", "were", "been", "just", "also", "more", "some", "many",
    "over", "than", "there", "users", "community", "game", "reddit", "post", "like", "would", "most",
    "much", "could", "should", "really", "still", "very", "make", "makes", "made", "stand",
}


def _has_negative_signal(text_blob: str) -> bool:
    return any(term in text_blob for term in NEGATIVE_SIGNAL_TERMS)


def _has_positive_signal(text_blob: str) -> bool:
    return any(term in text_blob for term in POSITIVE_SIGNAL_TERMS)


def _extract_theme_phrases_from_titles(posts: List[Dict[str, Any]], max_phrases: int = 6) -> List[str]:
    scored_phrases: Dict[str, float] = {}

    for post in posts:
        title = str(post.get("title", "") or "").lower()
        tokens = [
            token
            for token in re.findall(r"[a-z0-9]+", title)
            if len(token) > 2 and token not in THEME_STOP_WORDS
        ]
        if len(tokens) < 2:
            continue

        weight = _post_engagement_weight(post)
        for n in (3, 2):
            if len(tokens) < n:
                continue
            for idx in range(len(tokens) - n + 1):
                phrase = " ".join(tokens[idx : idx + n])
                if phrase in THEME_STOP_WORDS:
                    continue
                scored_phrases[phrase] = scored_phrases.get(phrase, 0.0) + weight

    ranked = sorted(scored_phrases.items(), key=lambda item: item[1], reverse=True)
    phrases = [phrase for phrase, _ in ranked[: max(max_phrases, 1)]]

    if phrases:
        return phrases

    fallback_phrases: List[str] = []
    for post in posts:
        title = str(post.get("title", "") or "").strip()
        if not title:
            continue
        words = [w for w in re.findall(r"[A-Za-z0-9]+", title) if len(w) > 2]
        if len(words) >= 2:
            phrase = " ".join(words[: min(4, len(words))]).lower()
            if phrase not in fallback_phrases:
                fallback_phrases.append(phrase)
        if len(fallback_phrases) >= max_phrases:
            break

    return fallback_phrases


def _build_schema_fallback(posts: List[Dict[str, Any]], game_name: str = "") -> Dict[str, Any]:
    ranked_posts = sorted(posts or [], key=_calculate_post_rank, reverse=True)
    top_posts = ranked_posts[:15]

    if not top_posts:
        return {
            "sentiment_label": "Mixed",
            "sentiment_summary": "Sentiment appears mixed, but there is not enough post data to produce a reliable breakdown.",
            "themes": [
                "Limited data - not enough high-signal posts to determine concrete product themes",
            ],
            "pain_points": [
                {"text": "Insufficient data to identify repeated pain points.", "evidence": []}
            ],
            "wins": [
                {"text": "Insufficient data to identify repeated wins.", "evidence": []}
            ],
        }

    positive_weight = 0.0
    negative_weight = 0.0
    for post in top_posts:
        blob = _post_signal_blob(post)
        weight = _post_engagement_weight(post)
        if _has_positive_signal(blob):
            positive_weight += weight
        if _has_negative_signal(blob):
            negative_weight += weight

    if negative_weight > positive_weight * 1.15:
        sentiment_label = "Negative"
    elif positive_weight > negative_weight * 1.15:
        sentiment_label = "Positive"
    else:
        sentiment_label = "Mixed"

    refs = [str(post.get("id") or "").strip() for post in top_posts if str(post.get("id") or "").strip()]
    ref_one = refs[0] if refs else ""
    ref_two = refs[1] if len(refs) > 1 else ref_one

    sentiment_summary = (
        f"Overall sentiment is {sentiment_label.lower()}, driven by repeated high-engagement product feedback. "
        f"Primary friction and upside signals are visible in [POST:{ref_one}] and [POST:{ref_two}] where available."
    ).strip()

    phrases = _extract_theme_phrases_from_titles(top_posts, max_phrases=6)
    if not phrases:
        phrases = ["gameplay feedback patterns", "content pacing concerns", "progression and balance issues"]

    themes: List[str] = []
    for idx, phrase in enumerate(phrases[:6]):
        ref = refs[idx % len(refs)] if refs else ""
        suffix = f" [POST:{ref}]" if ref else ""
        themes.append(
            f"{phrase.title()} - repeated player discussion with concrete product implications{suffix}"
        )

    negative_posts = [post for post in top_posts if _has_negative_signal(_post_signal_blob(post))]
    positive_posts = [post for post in top_posts if _has_positive_signal(_post_signal_blob(post))]
    if not negative_posts:
        negative_posts = top_posts[-5:] if len(top_posts) >= 5 else top_posts
    if not positive_posts:
        positive_posts = top_posts[:5]

    pain_points: List[Dict[str, Any]] = []
    wins: List[Dict[str, Any]] = []

    for idx in range(5):
        pain_post = negative_posts[idx % len(negative_posts)]
        win_post = positive_posts[idx % len(positive_posts)]

        pain_title = str(pain_post.get("title", "") or "Player-reported product issue").strip()
        pain_id = str(pain_post.get("id", "") or "").strip()
        pain_points.append(
            {
                "text": f"Players report friction around: {pain_title[:170]}",
                "evidence": [_format_permalink(pain_id)] if pain_id else [],
            }
        )

        win_title = str(win_post.get("title", "") or "Player-reported product strength").strip()
        win_id = str(win_post.get("id", "") or "").strip()
        wins.append(
            {
                "text": f"Players highlight a positive signal in: {win_title[:170]}",
                "evidence": [_format_permalink(win_id)] if win_id else [],
            }
        )

    return {
        "sentiment_label": sentiment_label,
        "sentiment_summary": sentiment_summary,
        "themes": themes,
        "pain_points": pain_points,
        "wins": wins,
    }


def _ensure_evidence_for_items(items: List[Dict[str, Any]], fallback_posts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not items:
        return []

    fallback_links = [
        _format_permalink(str(post.get("id") or "").strip())
        for post in sorted(fallback_posts or [], key=_calculate_post_rank, reverse=True)
        if str(post.get("id") or "").strip()
    ]

    ensured: List[Dict[str, Any]] = []
    for idx, item in enumerate(items):
        text_value = str(item.get("text", "") or "").strip()
        evidence = _normalize_evidence_links(item.get("evidence"))

        if not evidence:
            for post_id in _extract_post_ids(text_value):
                link = _format_permalink(post_id)
                if link not in evidence:
                    evidence.append(link)
                if len(evidence) >= 2:
                    break

        if not evidence and fallback_links:
            fallback_link = fallback_links[idx % len(fallback_links)]
            evidence.append(fallback_link)

        ensured.append({"text": text_value, "evidence": evidence[:2]})

    return ensured


def ensure_valid_analysis_schema(
    result: Dict[str, Any],
    fallback_posts: List[Dict[str, Any]],
    game_name: str = "",
) -> Dict[str, Any]:
    normalized = _normalize_analysis(result if isinstance(result, dict) else {})
    fallback = _build_schema_fallback(fallback_posts, game_name=game_name)

    sentiment_label = normalized.get("sentiment_label")
    if sentiment_label not in ("Positive", "Mixed", "Negative"):
        sentiment_label = fallback.get("sentiment_label", "Mixed")

    sentiment_summary = str(normalized.get("sentiment_summary", "") or "").strip()
    if not sentiment_summary:
        sentiment_summary = str(fallback.get("sentiment_summary", "") or "")

    themes = normalized.get("themes") or []
    if not themes:
        themes = fallback.get("themes") or []

    pain_points = normalized.get("pain_points") or []
    if not pain_points:
        pain_points = fallback.get("pain_points") or []
    pain_points = _ensure_evidence_for_items(pain_points[:5], fallback_posts)

    wins = normalized.get("wins") or []
    if not wins:
        wins = fallback.get("wins") or []
    wins = _ensure_evidence_for_items(wins[:5], fallback_posts)

    return {
        "sentiment_label": sentiment_label,
        "sentiment_summary": sentiment_summary,
        "themes": themes[:10],
        "pain_points": pain_points[:5],
        "wins": wins[:5],
    }


async def _repair_json_payload_with_ai(raw_text: str, schema_hint: str) -> Optional[Dict[str, Any]]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None

    raw_excerpt = str(raw_text or "").strip()
    if not raw_excerpt:
        return None

    try:
        import openai

        openai.api_key = api_key
        prompt = (
            "Convert the following model output into valid JSON only. Do not add commentary. "
            f"Schema hint: {schema_hint}.\n\n"
            "RAW OUTPUT:\n"
            + raw_excerpt[:3500]
        )

        response = await openai.ChatCompletion.acreate(
            model="gpt-4o-mini",
            messages=[
                {"role": "system", "content": "You repair invalid JSON. Return strict JSON only."},
                {"role": "user", "content": prompt},
            ],
            temperature=0.0,
            max_tokens=700,
        )

        repaired_text = response.choices[0].message.content or ""
        return _extract_json_payload(repaired_text)
    except Exception as exc:
        print(f"JSON repair failed ({schema_hint}): {exc}")
        return None


async def analyze_posts_with_ai(
    posts: List[Dict[str, Any]],
    comments: List[Dict[str, Any]],
    game_name: str = "",
    keywords: str = "",
) -> Dict[str, Any]:
    """Analyze Reddit posts/comments with OpenAI and return normalized sentiment output."""
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("OpenAI key missing; using deterministic analysis fallback.")
        return ensure_valid_analysis_schema({}, posts, game_name=game_name)

    try:
        import openai

        openai.api_key = api_key
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
            print(f"Overall analysis parse failed. Raw excerpt: {text[:300]!r}")
            repaired = await _repair_json_payload_with_ai(text, "overall_analysis")
            if repaired is not None:
                print("Overall analysis JSON repair used.")
                parsed = repaired

        if parsed is None:
            print("Overall analysis fallback used after parse/repair failure.")
            return ensure_valid_analysis_schema({}, posts, game_name=game_name)

        return ensure_valid_analysis_schema(parsed, posts, game_name=game_name)
    except Exception as exc:
        print(f"Overall analysis failed: {exc}")
        return ensure_valid_analysis_schema({}, posts, game_name=game_name)


async def analyze_subreddit_with_ai(
    posts: List[Dict[str, Any]],
    comments: List[Dict[str, Any]],
    subreddit_name: str,
    game_name: str = "",
    keywords: str = "",
) -> Dict[str, Any]:
    scoped_subreddit = _normalize_subreddit(subreddit_name) or subreddit_name
    scoped_game_name = game_name or "Unknown Game"
    scoped_label = f"{scoped_game_name} - r/{scoped_subreddit}" if scoped_subreddit else scoped_game_name
    return await analyze_posts_with_ai(posts, comments, game_name=scoped_label, keywords=keywords)


def _normalize_subreddit_list(subreddits: List[str], max_items: int = MAX_MULTI_SUBREDDITS) -> List[str]:
    unique: List[str] = []
    seen = set()

    for raw in subreddits:
        normalized = _normalize_subreddit(str(raw or ""))
        if not normalized:
            continue
        key = normalized.lower()
        if key in seen:
            continue
        seen.add(key)
        unique.append(normalized)
        if len(unique) >= max_items:
            break

    return unique


def _build_multi_scan_cache_key(
    subreddits: List[str],
    game_name: str,
    keywords: str,
    include_breakdown: bool,
) -> str:
    payload = {
        "subreddits": sorted([s.lower() for s in subreddits]),
        "game_name": _normalize_game_lookup_key(game_name),
        "keywords": " ".join(_tokenize_text(keywords)),
        "include_breakdown": bool(include_breakdown),
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _build_posts_by_subreddit(posts: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for post in posts:
        subreddit = _normalize_subreddit(str(post.get("subreddit", "") or ""))
        if not subreddit:
            continue
        grouped.setdefault(subreddit, []).append(post)

    for subreddit, subreddit_posts in list(grouped.items()):
        grouped[subreddit] = sorted(subreddit_posts, key=_calculate_post_rank, reverse=True)[
            :BREAKDOWN_MAX_POSTS_PER_SUBREDDIT
        ]

    return grouped


def _build_breakdown_prompt(
    posts_by_subreddit: Dict[str, List[Dict[str, Any]]],
    game_name: str,
    keywords: str,
) -> str:
    sections: List[str] = []

    for subreddit in sorted(posts_by_subreddit.keys()):
        posts = posts_by_subreddit.get(subreddit, [])[:BREAKDOWN_MAX_POSTS_PER_SUBREDDIT]
        lines = [f"SUBREDDIT: r/{subreddit}", f"POST_COUNT: {len(posts)}"]

        for idx, post in enumerate(posts):
            post_id = str(post.get("id") or "")
            title = str(post.get("title", "") or "").strip()
            score = int(post.get("score", 0) or 0)
            num_comments = int(post.get("num_comments", 0) or 0)
            lines.append(f"- [POST:{post_id}] [{score} pts, {num_comments} comments] {title}")

            if idx < 4:
                selftext = str(post.get("selftext", "") or "").strip()
                if selftext and selftext not in ("[removed]", "[deleted]"):
                    snippet = selftext.replace("\n", " ")[:BREAKDOWN_SELFTEXT_TRUNCATE].strip()
                    if snippet:
                        lines.append(f"  Snippet: {snippet}")

        sections.append("\n".join(lines))

    keyword_note = f"\nKeywords to watch for: {keywords}" if keywords else ""

    return f"""Create a per-subreddit product feedback breakdown for "{game_name or 'Unknown Game'}".

OUTPUT JSON ONLY with this exact top-level shape:
{{
  "breakdown": [
    {{
      "subreddit": "name",
      "sentiment_label": "Positive|Mixed|Negative",
      "summary_bullets": ["...", "...", "..."],
      "top_themes": ["Theme - concrete issue/outcome [POST:id]", "..."],
      "top_pain_points": [{{"text": "...", "evidence": ["https://www.reddit.com/comments/POST_ID/"]}}],
      "top_wins": [{{"text": "...", "evidence": ["https://www.reddit.com/comments/POST_ID/"]}}]
    }}
  ]
}}

STRICT RULES:
- Use only supplied posts.
- Do NOT assume game genre, modes, platforms, monetisation, or mechanics unless explicitly present.
- summary_bullets: max 3
- top_themes: 3-5 and must be specific (not generic labels like "Gameplay Mechanics")
- top_pain_points: exactly 3, product-focused
- top_wins: exactly 3, product-focused
- Evidence arrays MUST contain full Reddit URLs only: https://www.reddit.com/comments/POST_ID/
- Never use placeholders like [source 1]
- Never output [POST:id] inside evidence arrays (only in themes/summary text)
{keyword_note}

SUBREDDIT DATA:
{chr(10).join(sections)}

Return valid JSON only. No markdown fences.
"""


def _normalize_summary_bullets(value: Any) -> List[str]:
    if isinstance(value, list):
        bullets = [str(item).strip() for item in value if str(item).strip()]
        return bullets[:3]

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return []
        segments = [seg.strip() for seg in re.split(r"[\n\r]+|\.\s+", text) if seg.strip()]
        return segments[:3]

    return []


def _extract_post_ids(text: str) -> List[str]:
    return re.findall(r"\[POST:([A-Za-z0-9_]+)\]", text or "")


def _normalize_breakdown_items(value: Any) -> List[Dict[str, Any]]:
    if not isinstance(value, list):
        return []

    items: List[Dict[str, Any]] = []
    for raw_item in value:
        if not isinstance(raw_item, dict):
            continue

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

        candidate_ids: List[str] = []
        for key in ("post_id", "source_post_id", "id"):
            raw_id = str(raw_item.get(key) or "").strip()
            if raw_id and raw_id not in candidate_ids:
                candidate_ids.append(raw_id)

        for post_id in _extract_post_ids(text):
            if post_id not in candidate_ids:
                candidate_ids.append(post_id)

        if not evidence and candidate_ids:
            for post_id in candidate_ids:
                link = _format_permalink(post_id)
                if link not in evidence:
                    evidence.append(link)
                if len(evidence) >= 2:
                    break

        items.append({"text": text, "evidence": evidence[:2]})

    return items[:3]


def _normalize_breakdown_payload(payload: Any) -> Dict[str, Any]:
    raw_breakdown: Any = None

    if isinstance(payload, dict):
        raw_breakdown = payload.get("breakdown")
        if not isinstance(raw_breakdown, list):
            for key in ("rows", "subreddits", "subreddit_breakdown"):
                candidate = payload.get(key)
                if isinstance(candidate, list):
                    raw_breakdown = candidate
                    break
                if isinstance(candidate, dict) and isinstance(candidate.get("breakdown"), list):
                    raw_breakdown = candidate.get("breakdown")
                    break
    elif isinstance(payload, list):
        raw_breakdown = payload

    if not isinstance(raw_breakdown, list):
        return {"breakdown": []}

    normalized_rows: List[Dict[str, Any]] = []

    for item in raw_breakdown:
        if not isinstance(item, dict):
            continue

        subreddit = _normalize_subreddit(str(item.get("subreddit", "") or ""))
        if not subreddit:
            continue

        sentiment_label = _normalize_sentiment_label(item.get("sentiment_label"))
        if sentiment_label == "Unknown":
            sentiment_label = "Mixed"

        summary_bullets = _normalize_summary_bullets(item.get("summary_bullets"))
        themes = _normalize_themes(item.get("top_themes"))[:5]
        pain_points = _normalize_breakdown_items(item.get("top_pain_points"))
        wins = _normalize_breakdown_items(item.get("top_wins"))

        normalized_rows.append(
            {
                "subreddit": subreddit,
                "sentiment_label": sentiment_label,
                "summary_bullets": summary_bullets,
                "top_themes": themes,
                "top_pain_points": pain_points,
                "top_wins": wins,
            }
        )

    return {"breakdown": normalized_rows}


def _estimate_sentiment_from_posts(posts: List[Dict[str, Any]]) -> str:
    if not posts:
        return "Mixed"

    positive_weight = 0.0
    negative_weight = 0.0

    for post in posts:
        blob = _post_signal_blob(post)
        weight = _post_engagement_weight(post)
        if _has_positive_signal(blob):
            positive_weight += weight
        if _has_negative_signal(blob):
            negative_weight += weight

    if negative_weight > positive_weight * 1.15:
        return "Negative"
    if positive_weight > negative_weight * 1.15:
        return "Positive"
    return "Mixed"


def _extract_theme_terms(posts: List[Dict[str, Any]], max_terms: int = 5) -> List[str]:
    # Use phrases from titles rather than isolated tokens to avoid noisy outputs.
    return _extract_theme_phrases_from_titles(posts, max_phrases=max(max_terms, 1))


def _fallback_point_from_post(post: Dict[str, Any], prefix: str) -> Dict[str, Any]:
    title = str(post.get("title", "") or "").strip()
    post_id = str(post.get("id", "") or "").strip()
    evidence = [_format_permalink(post_id)] if post_id else []

    if not title:
        title = "player-reported product feedback"

    return {
        "text": f"{prefix}: {title[:180]}",
        "evidence": evidence,
    }


def _build_fallback_breakdown_rows(
    posts_by_subreddit: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []

    for subreddit in sorted(posts_by_subreddit.keys()):
        posts = sorted(posts_by_subreddit.get(subreddit, []), key=_calculate_post_rank, reverse=True)
        if not posts:
            continue

        top_posts = posts[:BREAKDOWN_MAX_POSTS_PER_SUBREDDIT]
        sentiment_label = _estimate_sentiment_from_posts(top_posts)
        post_refs = [str(post.get("id") or "").strip() for post in top_posts if str(post.get("id") or "").strip()]

        ref_one = post_refs[0] if post_refs else ""
        ref_two = post_refs[1] if len(post_refs) > 1 else ref_one

        phrases = _extract_theme_terms(top_posts, max_terms=5)
        if not phrases:
            phrases = ["content pacing feedback", "difficulty tuning feedback", "progression friction reports"]

        summary_bullets = [
            f"Overall sentiment in r/{subreddit} is {sentiment_label.lower()} based on high-engagement product discussions.",
            f"Most repeated subtopics include {', '.join(phrases[:3])}.",
            f"Representative threads: [POST:{ref_one}], [POST:{ref_two}]" if ref_one else "Representative threads are available in this community sample.",
        ]

        top_themes: List[str] = []
        for idx, phrase in enumerate(phrases[:5]):
            ref = post_refs[idx % len(post_refs)] if post_refs else ""
            suffix = f" [POST:{ref}]" if ref else ""
            top_themes.append(
                f"{phrase.title()} - recurring product feedback trend in high-engagement posts{suffix}"
            )
        while len(top_themes) < 3:
            ref = post_refs[len(top_themes) % len(post_refs)] if post_refs else ""
            suffix = f" [POST:{ref}]" if ref else ""
            top_themes.append(f"Product Feedback Signal - repeated issue/outcome in recent threads{suffix}")

        negative_posts = [post for post in top_posts if _has_negative_signal(_post_signal_blob(post))]
        positive_posts = [post for post in top_posts if _has_positive_signal(_post_signal_blob(post))]
        if not negative_posts:
            negative_posts = top_posts[-3:] if len(top_posts) >= 3 else top_posts
        if not positive_posts:
            positive_posts = top_posts[:3]

        pain_points: List[Dict[str, Any]] = []
        wins: List[Dict[str, Any]] = []
        for idx in range(3):
            pain_post = negative_posts[idx % len(negative_posts)]
            win_post = positive_posts[idx % len(positive_posts)]
            pain_points.append(_fallback_point_from_post(pain_post, "Players report friction around"))
            wins.append(_fallback_point_from_post(win_post, "Players praise"))

        rows.append(
            {
                "subreddit": subreddit,
                "sentiment_label": sentiment_label,
                "summary_bullets": summary_bullets[:3],
                "top_themes": top_themes[:5],
                "top_pain_points": pain_points[:3],
                "top_wins": wins[:3],
            }
        )

    return rows


def _build_fallback_breakdown(
    posts_by_subreddit: Dict[str, List[Dict[str, Any]]],
    error_message: str,
) -> Dict[str, Any]:
    print(f"Breakdown fallback used: {error_message}")
    return {"error": "fallback_generated", "breakdown": _build_fallback_breakdown_rows(posts_by_subreddit)}


def _merge_breakdown_with_fallback(
    normalized_payload: Dict[str, Any],
    posts_by_subreddit: Dict[str, List[Dict[str, Any]]],
) -> List[Dict[str, Any]]:
    fallback_rows_by_subreddit = {
        row.get("subreddit"): row
        for row in _build_fallback_breakdown_rows(posts_by_subreddit)
        if isinstance(row, dict) and row.get("subreddit")
    }

    parsed_rows_by_subreddit: Dict[str, Dict[str, Any]] = {}
    for row in normalized_payload.get("breakdown", []):
        if not isinstance(row, dict):
            continue
        subreddit = _normalize_subreddit(str(row.get("subreddit", "") or ""))
        if not subreddit:
            continue
        parsed_rows_by_subreddit[subreddit] = row

    merged_rows: List[Dict[str, Any]] = []

    for subreddit in sorted(posts_by_subreddit.keys()):
        fallback_row = fallback_rows_by_subreddit.get(subreddit)
        if not fallback_row:
            continue

        parsed_row = parsed_rows_by_subreddit.get(subreddit)
        if not parsed_row:
            merged_rows.append(fallback_row)
            continue

        sentiment_label = _normalize_sentiment_label(parsed_row.get("sentiment_label"))
        if sentiment_label == "Unknown":
            sentiment_label = str(fallback_row.get("sentiment_label", "Mixed"))

        summary_bullets = _normalize_summary_bullets(parsed_row.get("summary_bullets"))
        top_themes = _normalize_themes(parsed_row.get("top_themes"))[:5]
        top_pain_points = _normalize_breakdown_items(parsed_row.get("top_pain_points"))
        top_wins = _normalize_breakdown_items(parsed_row.get("top_wins"))

        subreddit_posts = posts_by_subreddit.get(subreddit, [])
        top_pain_points = _ensure_evidence_for_items(top_pain_points, subreddit_posts)[:3]
        top_wins = _ensure_evidence_for_items(top_wins, subreddit_posts)[:3]

        quality_ok = (
            bool(summary_bullets)
            and len(top_themes) >= 3
            and len(top_pain_points) >= 3
            and len(top_wins) >= 3
        )

        if not quality_ok:
            merged_rows.append(fallback_row)
            continue

        merged_rows.append(
            {
                "subreddit": subreddit,
                "sentiment_label": sentiment_label,
                "summary_bullets": summary_bullets[:3],
                "top_themes": top_themes,
                "top_pain_points": top_pain_points,
                "top_wins": top_wins,
            }
        )

    return merged_rows


def _build_subreddit_breakdown_cache_key(
    subreddit: str,
    posts: List[Dict[str, Any]],
    game_name: str,
    keywords: str,
) -> str:
    ranked_posts = sorted(posts or [], key=_calculate_post_rank, reverse=True)[:BREAKDOWN_MAX_POSTS_PER_SUBREDDIT]
    top_post_ids = [str(post.get("id") or "").strip() for post in ranked_posts if str(post.get("id") or "").strip()]
    payload = {
        "subreddit": _normalize_subreddit(subreddit).lower(),
        "game_name": _normalize_game_lookup_key(game_name),
        "keywords": " ".join(_tokenize_text(keywords)),
        "post_ids": top_post_ids,
    }
    return json.dumps(payload, sort_keys=True, separators=(",", ":"))


def _extract_post_id_from_evidence_link(link: str) -> str:
    match = re.search(r"reddit\.com/comments/([a-z0-9_]+)", str(link or ""), re.IGNORECASE)
    if match:
        return str(match.group(1)).strip()
    return ""


def _first_sentence(text: str) -> str:
    value = str(text or "").strip()
    if not value:
        return ""

    parts = [part.strip() for part in re.split(r"(?<=[.!?])\s+", value) if part.strip()]
    if parts:
        return parts[0]

    return value[:220].strip()


def _extract_representative_post_ids(
    pain_points: List[Dict[str, Any]],
    wins: List[Dict[str, Any]],
    posts: List[Dict[str, Any]],
    max_ids: int = 2,
) -> List[str]:
    ids: List[str] = []

    for item in (pain_points + wins):
        evidence = _normalize_evidence_links(item.get("evidence")) if isinstance(item, dict) else []
        for link in evidence:
            post_id = _extract_post_id_from_evidence_link(link)
            if post_id and post_id not in ids:
                ids.append(post_id)
            if len(ids) >= max_ids:
                return ids

    for post in sorted(posts or [], key=_calculate_post_rank, reverse=True):
        post_id = str(post.get("id") or "").strip()
        if post_id and post_id not in ids:
            ids.append(post_id)
        if len(ids) >= max_ids:
            break

    return ids[:max_ids]


def _build_single_subreddit_fallback_row(subreddit: str, posts: List[Dict[str, Any]]) -> Dict[str, Any]:
    normalized_subreddit = _normalize_subreddit(subreddit) or subreddit
    fallback_rows = _build_fallback_breakdown_rows({normalized_subreddit: posts or []})
    if fallback_rows:
        return fallback_rows[0]

    return {
        "subreddit": normalized_subreddit,
        "sentiment_label": "Mixed",
        "summary_bullets": [
            f"Overall sentiment in r/{normalized_subreddit} is mixed.",
            "Insufficient subreddit-level data to produce a reliable AI summary.",
            "Representative threads are available in the sampled posts.",
        ],
        "top_themes": [
            "Insufficient subreddit signal - not enough high-engagement product feedback",
            "Insufficient subreddit signal - unable to extract repeated subtopics",
            "Insufficient subreddit signal - rerun scan after more activity",
        ],
        "top_pain_points": [
            {"text": "Insufficient data to identify repeated pain points.", "evidence": []},
            {"text": "Insufficient data to identify repeated pain points.", "evidence": []},
            {"text": "Insufficient data to identify repeated pain points.", "evidence": []},
        ],
        "top_wins": [
            {"text": "Insufficient data to identify repeated wins.", "evidence": []},
            {"text": "Insufficient data to identify repeated wins.", "evidence": []},
            {"text": "Insufficient data to identify repeated wins.", "evidence": []},
        ],
    }


def _map_analysis_to_breakdown_row(
    subreddit: str,
    posts: List[Dict[str, Any]],
    analysis: Dict[str, Any],
) -> Dict[str, Any]:
    fallback_row = _build_single_subreddit_fallback_row(subreddit, posts)

    sentiment_label = _normalize_sentiment_label(analysis.get("sentiment_label"))
    if sentiment_label == "Unknown":
        sentiment_label = str(fallback_row.get("sentiment_label") or "Mixed")

    themes = _normalize_themes(analysis.get("themes"))[:5]
    fallback_themes = [str(item).strip() for item in fallback_row.get("top_themes", []) if str(item).strip()]
    for fallback_theme in fallback_themes:
        if len(themes) >= 3:
            break
        if fallback_theme not in themes:
            themes.append(fallback_theme)
    themes = themes[:5]

    pain_points = _normalize_insight_items(analysis.get("pain_points"))[:3]
    pain_points = _ensure_evidence_for_items(pain_points, posts)[:3]
    fallback_pain = fallback_row.get("top_pain_points", []) if isinstance(fallback_row.get("top_pain_points"), list) else []
    while len(pain_points) < 3:
        idx = len(pain_points)
        candidate = fallback_pain[idx % len(fallback_pain)] if fallback_pain else {"text": "Insufficient data to identify repeated pain points.", "evidence": []}
        pain_points.append({
            "text": str(candidate.get("text") or "Insufficient data to identify repeated pain points."),
            "evidence": _normalize_evidence_links(candidate.get("evidence")),
        })

    wins = _normalize_insight_items(analysis.get("wins"))[:3]
    wins = _ensure_evidence_for_items(wins, posts)[:3]
    fallback_wins = fallback_row.get("top_wins", []) if isinstance(fallback_row.get("top_wins"), list) else []
    while len(wins) < 3:
        idx = len(wins)
        candidate = fallback_wins[idx % len(fallback_wins)] if fallback_wins else {"text": "Insufficient data to identify repeated wins.", "evidence": []}
        wins.append({
            "text": str(candidate.get("text") or "Insufficient data to identify repeated wins."),
            "evidence": _normalize_evidence_links(candidate.get("evidence")),
        })

    summary_text = _first_sentence(str(analysis.get("sentiment_summary") or ""))
    if not summary_text:
        fallback_summary = fallback_row.get("summary_bullets", []) if isinstance(fallback_row.get("summary_bullets"), list) else []
        summary_text = str(fallback_summary[1] if len(fallback_summary) > 1 else "Subreddit-level signal was extracted from top community threads.")

    representative_ids = _extract_representative_post_ids(pain_points, wins, posts, max_ids=2)
    if representative_ids:
        if len(representative_ids) == 1:
            representative_line = f"Representative threads: [POST:{representative_ids[0]}]"
        else:
            representative_line = f"Representative threads: [POST:{representative_ids[0]}], [POST:{representative_ids[1]}]"
    else:
        representative_line = "Representative threads are available in the sampled posts."

    return {
        "subreddit": _normalize_subreddit(subreddit) or subreddit,
        "sentiment_label": sentiment_label,
        "summary_bullets": [
            f"Overall sentiment in r/{_normalize_subreddit(subreddit) or subreddit} is {sentiment_label.lower()}.",
            summary_text,
            representative_line,
        ][:3],
        "top_themes": themes[:5],
        "top_pain_points": pain_points[:3],
        "top_wins": wins[:3],
    }


async def _analyze_one_subreddit_breakdown_row(
    subreddit: str,
    posts: List[Dict[str, Any]],
    game_name: str,
    keywords: str,
    semaphore: asyncio.Semaphore,
) -> Dict[str, Any]:
    normalized_subreddit = _normalize_subreddit(subreddit) or subreddit
    ranked_posts = sorted(posts or [], key=_calculate_post_rank, reverse=True)[:BREAKDOWN_MAX_POSTS_PER_SUBREDDIT]
    if not ranked_posts:
        return _build_single_subreddit_fallback_row(normalized_subreddit, ranked_posts)

    cache_key = _build_subreddit_breakdown_cache_key(
        normalized_subreddit,
        ranked_posts,
        game_name=game_name,
        keywords=keywords,
    )

    now = time.time()
    cached_row = _subreddit_breakdown_cache.get(cache_key)
    if cached_row and now - cached_row[0] < MULTI_SCAN_CACHE_TTL:
        return cached_row[1]

    try:
        try:
            subreddit_comments = await sample_comments_for_posts(
                ranked_posts,
                max_posts=min(6, TOP_POSTS_FOR_COMMENTS),
                max_comments_per_post=MAX_COMMENTS_PER_POST,
            )
        except Exception:
            subreddit_comments = []

        async with semaphore:
            analysis = await analyze_subreddit_with_ai(
                ranked_posts,
                subreddit_comments,
                subreddit_name=normalized_subreddit,
                game_name=game_name,
                keywords=keywords,
            )

        row = _map_analysis_to_breakdown_row(normalized_subreddit, ranked_posts, analysis)
        _subreddit_breakdown_cache[cache_key] = (now, row)
        return row
    except Exception as exc:
        print(f"Subreddit breakdown failed for r/{normalized_subreddit}: {exc}")
        row = _build_single_subreddit_fallback_row(normalized_subreddit, ranked_posts)
        _subreddit_breakdown_cache[cache_key] = (now, row)
        return row


async def analyze_subreddit_breakdown_with_ai(
    posts_by_subreddit: Dict[str, List[Dict[str, Any]]],
    game_name: str = "",
    keywords: str = "",
) -> Dict[str, Any]:
    if not posts_by_subreddit:
        return {"breakdown": []}

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return {
            "error": "fallback_generated",
            "breakdown": _build_fallback_breakdown_rows(posts_by_subreddit),
        }

    semaphore = asyncio.Semaphore(2)
    ordered_subreddits = sorted(posts_by_subreddit.keys())

    tasks = [
        _analyze_one_subreddit_breakdown_row(
            subreddit,
            posts_by_subreddit.get(subreddit, []),
            game_name=game_name,
            keywords=keywords,
            semaphore=semaphore,
        )
        for subreddit in ordered_subreddits
    ]

    rows: List[Dict[str, Any]] = []
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for idx, result in enumerate(results):
        subreddit = ordered_subreddits[idx]
        subreddit_posts = posts_by_subreddit.get(subreddit, [])

        if isinstance(result, Exception):
            print(f"Subreddit breakdown task failed for r/{subreddit}: {result}")
            rows.append(_build_single_subreddit_fallback_row(subreddit, subreddit_posts))
            continue

        if not isinstance(result, dict):
            rows.append(_build_single_subreddit_fallback_row(subreddit, subreddit_posts))
            continue

        rows.append(result)

    return {"breakdown": rows}


def _public_multi_scan_result(result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "overall": result.get("overall") or {},
        "meta": result.get("meta") or {},
        "subreddit_breakdown": result.get("subreddit_breakdown") or {"breakdown": []},
    }


async def scan_multiple_subreddits(
    subreddits: List[str],
    game_name: str = "",
    keywords: str = "",
    include_breakdown: bool = True,
    include_internal: bool = False,
) -> Dict[str, Any]:
    normalized_subreddits = _normalize_subreddit_list(subreddits, max_items=MAX_MULTI_SUBREDDITS)
    if not normalized_subreddits:
        raise RuntimeError("At least one valid subreddit is required")

    cache_key = _build_multi_scan_cache_key(
        normalized_subreddits,
        game_name=game_name,
        keywords=keywords,
        include_breakdown=include_breakdown,
    )

    now = time.time()
    cached = _multi_scan_cache.get(cache_key)
    if cached and now - cached[0] < MULTI_SCAN_CACHE_TTL:
        cached_result = cached[1]
        if include_internal:
            return cached_result
        return _public_multi_scan_result(cached_result)

    posts = await fetch_posts_for_subreddits(
        normalized_subreddits,
        per_sub_limit=80,
        total_limit=150,
    )
    if not posts:
        joined = ", ".join([f"r/{sub}" for sub in normalized_subreddits])
        raise RuntimeError(f"No posts found for selected subreddits: {joined}")

    try:
        comments = await sample_comments_for_posts(
            posts,
            max_posts=TOP_POSTS_FOR_COMMENTS,
            max_comments_per_post=MAX_COMMENTS_PER_POST,
        )
    except Exception:
        comments = []

    overall = await analyze_posts_with_ai(
        posts,
        comments,
        game_name=game_name,
        keywords=keywords,
    )

    posts_by_subreddit = _build_posts_by_subreddit(posts)
    subreddit_breakdown: Dict[str, Any] = {"breakdown": []}

    if include_breakdown:
        subreddit_breakdown = await analyze_subreddit_breakdown_with_ai(
            posts_by_subreddit,
            game_name=game_name,
            keywords=keywords,
        )

    result = {
        "overall": overall,
        "meta": {
            "subreddits": normalized_subreddits,
            "posts_analysed": len(posts),
            "comments_sampled": len(comments),
            "last_scanned": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        },
        "subreddit_breakdown": subreddit_breakdown,
        "_posts": posts,
        "_comments": comments,
    }

    _multi_scan_cache[cache_key] = (now, result)
    if include_internal:
        return result
    return _public_multi_scan_result(result)


