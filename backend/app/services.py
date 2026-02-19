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
DISCOVERY_CACHE_TTL = 24 * 60 * 60  # 24 hours
WINDOWS: List[Tuple[str, str]] = [("48h", "0h"), ("8d", "48h"), ("30d", "8d")]

MAX_POSTS_FINAL = 100
MAX_POSTS_PER_AUTHOR = 3
MAX_NO_COMMENT_POSTS = 20
MIN_RECENT_POSTS = 20
DISCOVERY_MAX_RESULTS = 10
DISCOVERY_MAX_CANDIDATES = 30
DISCOVERY_SAMPLE_POSTS = 25
DISCOVERY_OPENAI_TOP = 10

TOP_POSTS_FOR_COMMENTS = 15
MAX_COMMENTS_PER_POST = 10
COMMENT_BODY_TRUNCATE = 400
POST_SELFTEXT_TRUNCATE = 500
COMMENT_FETCH_DELAY = 0.2

_post_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
_comments_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}
_discovery_cache: Dict[str, Tuple[float, List[Dict[str, Any]]]] = {}


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

