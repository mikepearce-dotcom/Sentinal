import asyncio
import json
import os
import re
import time
from typing import Any, Dict, List

from .services_analysis import (
    _ensure_evidence_for_items,
    _extract_post_ids,
    _extract_theme_phrases_from_titles,
    _has_negative_signal,
    _has_positive_signal,
    _normalize_evidence_links,
    _normalize_insight_items,
    _normalize_sentiment_label,
    _normalize_themes,
    _post_signal_blob,
    analyze_posts_with_ai,
    analyze_subreddit_with_ai,
)
from .services_common import (
    BREAKDOWN_MAX_POSTS_PER_SUBREDDIT,
    BREAKDOWN_SELFTEXT_TRUNCATE,
    MAX_COMMENTS_PER_POST,
    MAX_MULTI_SUBREDDITS,
    MULTI_SCAN_CACHE_TTL,
    TOP_POSTS_FOR_COMMENTS,
    _calculate_post_rank,
    _format_permalink,
    _multi_scan_cache,
    _normalize_game_lookup_key,
    _normalize_subreddit,
    _subreddit_breakdown_cache,
    _tokenize_text,
)
from .services_fetch import fetch_posts_for_subreddits, sample_comments_for_posts

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


