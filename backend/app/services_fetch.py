import asyncio
import math
import os
import re
import time
from typing import Any, Dict, List, Optional

import httpx

from .services_common import (
    ARCTIC_SHIFT_BASE,
    COMMENT_FIELDS,
    COMMENT_FETCH_DELAY,
    CACHE_TTL,
    DISCOVERY_CACHE_TTL,
    DISCOVERY_MAX_CANDIDATES,
    DISCOVERY_MAX_RESULTS,
    DISCOVERY_OPENAI_TOP,
    DISCOVERY_SAMPLE_POSTS,
    MAX_COMMENTS_PER_POST,
    MAX_POSTS_FINAL,
    TOP_POSTS_FOR_COMMENTS,
    POST_FIELDS,
    WINDOWS,
    _apply_diversity_and_recency,
    _apply_quality_filter,
    _build_subreddit_prefixes,
    _calculate_post_rank,
    _comments_cache,
    _discovery_cache,
    _extract_error_detail,
    _extract_json_payload,
    _map_post,
    _normalize_game_lookup_key,
    _normalize_subreddit,
    _post_cache,
    _select_best_comments,
    _tokenize_text,
)

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
    game_token_set = {token for token in game_tokens if len(token) >= 3}
    if not game_token_set:
        game_token_set = set(game_tokens)
    weighted_matches = 0.0
    for post in posts:
        title = str(post.get("title", "") or "")
        selftext = str(post.get("selftext", "") or "")[:260]
        post_tokens = set(_tokenize_text(f"{title} {selftext}"))
        overlap = post_tokens.intersection(game_token_set)
        if not overlap:
            continue
        # Stronger signal when multiple game tokens appear in the same post.
        if len(overlap) >= 2:
            weighted_matches += 1.0
        else:
            weighted_matches += 0.6
    return min(1.0, weighted_matches / float(len(posts)))

def _strict_name_match_score(game_name: str, candidate: Dict[str, Any]) -> float:
    normalized_game = "".join(_tokenize_text(game_name))
    if not normalized_game:
        return 0.0
    subreddit = "".join(_tokenize_text(str(candidate.get("subreddit", "") or "")))
    title = "".join(_tokenize_text(str(candidate.get("title", "") or "")))
    description = "".join(_tokenize_text(str(candidate.get("description", "") or "")))
    if normalized_game and normalized_game == subreddit:
        return 1.0
    if normalized_game and normalized_game in subreddit:
        return 0.85
    if normalized_game and normalized_game in title:
        return 0.7
    if normalized_game and normalized_game in description:
        return 0.45
    return 0.0

def _normalize_activity_score(raw_value: float, low: float, high: float) -> float:
    if raw_value <= 0:
        return 0.0
    if high <= low:
        return 0.5
    normalized = (raw_value - low) / (high - low)
    if normalized < 0.0:
        return 0.0
    if normalized > 1.0:
        return 1.0
    return normalized

def _build_discovery_reason(
    content_score: float,
    activity_score: float,
    name_score: float,
    strict_match_score: float,
) -> str:
    if strict_match_score >= 0.8 and content_score >= 0.25:
        return "Direct name match with relevant recent discussion"
    if name_score >= 0.55 and content_score >= 0.30:
        return "Strong title/name relevance with supporting discussion signal"
    if content_score >= 0.45 and activity_score >= 0.45:
        return "Frequent recent game mentions with healthy activity"
    if strict_match_score >= 0.6:
        return "Likely official or close-match community by name"
    if name_score >= 0.35 and activity_score >= 0.35:
        return "Relevant match with moderate engagement"
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
    game_tokens = _extract_signal_tokens(game_name)
    pre_scored: List[Dict[str, Any]] = []
    for candidate in candidate_map.values():
        subreddit = str(candidate.get("subreddit", "") or "")
        if not subreddit:
            continue
        name_score = _name_similarity_score(game_tokens, candidate)
        strict_match_score = _strict_name_match_score(game_name, candidate)
        subscribers = int(candidate.get("subscribers", 0) or 0)
        # Relevance-first gate: avoid low-signal communities before activity weighting.
        if strict_match_score < 0.45 and name_score < 0.20:
            continue
        pre_scored.append(
            {
                **candidate,
                "subscribers": subscribers,
                "_name_score": name_score,
                "_strict_match_score": strict_match_score,
                "_relevance_seed": (0.65 * strict_match_score) + (0.35 * name_score),
            }
        )
    # Fallback so discovery still returns candidates even when strict matching is sparse.
    if not pre_scored:
        fallback = sorted(
            candidate_map.values(),
            key=lambda item: int(item.get("subscribers", 0) or 0),
            reverse=True,
        )[: max(6, min(DISCOVERY_MAX_CANDIDATES, 12))]
        for candidate in fallback:
            name_score = _name_similarity_score(game_tokens, candidate)
            strict_match_score = _strict_name_match_score(game_name, candidate)
            pre_scored.append(
                {
                    **candidate,
                    "subscribers": int(candidate.get("subscribers", 0) or 0),
                    "_name_score": name_score,
                    "_strict_match_score": strict_match_score,
                    "_relevance_seed": (0.65 * strict_match_score) + (0.35 * name_score),
                }
            )
    ranked_candidates = sorted(
        pre_scored,
        key=lambda item: (
            float(item.get("_relevance_seed", 0.0)),
            int(item.get("subscribers", 0) or 0),
        ),
        reverse=True,
    )[:DISCOVERY_MAX_CANDIDATES]
    scored: List[Dict[str, Any]] = []
    for candidate in ranked_candidates:
        subreddit = str(candidate.get("subreddit", "") or "")
        if not subreddit:
            continue
        recent_posts: List[Dict[str, Any]] = []
        baseline_posts: List[Dict[str, Any]] = []
        try:
            recent_posts = await _fetch_posts_window(subreddit, after="14d", before="0h")
        except Exception as exc:
            print(f"Subreddit recent sample fetch failed ({subreddit}): {exc}")
        try:
            baseline_posts = await _fetch_posts_window(subreddit, after="30d", before="14d")
        except Exception as exc:
            print(f"Subreddit baseline sample fetch failed ({subreddit}): {exc}")
        recent_limit = max(1, int(DISCOVERY_SAMPLE_POSTS * 0.7))
        recent_posts = recent_posts[:recent_limit]
        remaining = max(DISCOVERY_SAMPLE_POSTS - len(recent_posts), 0)
        baseline_posts = baseline_posts[:remaining]
        sampled_posts = (recent_posts + baseline_posts)[:DISCOVERY_SAMPLE_POSTS]
        total_comments = sum(int(post.get("num_comments", 0) or 0) for post in sampled_posts)
        total_score = sum(int(post.get("score", 0) or 0) for post in sampled_posts)
        raw_activity = (
            math.log(1 + total_comments)
            + 0.4 * math.log(1 + total_score)
            + 0.2 * math.log(1 + int(candidate.get("subscribers", 0) or 0))
        )
        name_score = float(candidate.get("_name_score", 0.0) or 0.0)
        strict_match_score = float(candidate.get("_strict_match_score", 0.0) or 0.0)
        content_score = _content_relevance_score(game_tokens, sampled_posts)
        titles_source = recent_posts if recent_posts else sampled_posts
        scored.append(
            {
                "subreddit": subreddit,
                "subscribers": int(candidate.get("subscribers", 0) or 0),
                "score": 0.0,
                "reason": "",
                "_name_score": name_score,
                "_strict_match_score": strict_match_score,
                "_content_score": content_score,
                "_raw_activity": raw_activity,
                "_sample_titles": [str(post.get("title", "") or "") for post in titles_source[:3]],
            }
        )
    if not scored:
        _discovery_cache[lookup_key] = (now, [])
        return []
    activity_values = [float(item.get("_raw_activity", 0.0) or 0.0) for item in scored]
    activity_low = min(activity_values) if activity_values else 0.0
    activity_high = max(activity_values) if activity_values else 0.0
    for item in scored:
        content_score = float(item.get("_content_score", 0.0) or 0.0)
        name_score = float(item.get("_name_score", 0.0) or 0.0)
        strict_match_score = float(item.get("_strict_match_score", 0.0) or 0.0)
        activity_score = _normalize_activity_score(
            float(item.get("_raw_activity", 0.0) or 0.0),
            activity_low,
            activity_high,
        )
        combined_score = (
            (0.50 * content_score)
            + (0.22 * name_score)
            + (0.20 * strict_match_score)
            + (0.08 * activity_score)
        )
        if content_score >= 0.55 and strict_match_score >= 0.60:
            combined_score += 0.08
        item["score"] = round(combined_score, 4)
        item["reason"] = _build_discovery_reason(
            content_score,
            activity_score,
            name_score,
            strict_match_score,
        )
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



