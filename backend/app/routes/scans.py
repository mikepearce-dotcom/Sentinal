import math
import os
import re
import uuid
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from .. import database, services
from ..models import ScanCompareOut, ScanResultDetailOut, ScanResultOut, ScanTrendPointOut, ScanTrendsOut
from ..security import allow_request, client_ip, parse_int_env
from .auth import get_current_user

router = APIRouter()

SCAN_RATE_LIMIT = parse_int_env(os.getenv("SCAN_RATE_LIMIT"), default=20)
SCAN_RATE_WINDOW_SECONDS = parse_int_env(os.getenv("SCAN_RATE_WINDOW_SECONDS"), default=300)


class MultiScanRequest(BaseModel):
    subreddits: List[str] = Field(default_factory=list, min_items=1, max_items=5)
    game_id: str = ""
    game_name: str = ""
    keywords: str = ""
    include_breakdown: bool = True


def _safe_list(value: Any) -> List[Dict[str, Any]]:
    return value if isinstance(value, list) else []


def _enforce_scan_rate_limit(request: Request, user_id: str, scope: str) -> None:
    key = f"scan:{scope}:{user_id}:{client_ip(request)}"
    if allow_request(key, limit=SCAN_RATE_LIMIT, window_seconds=SCAN_RATE_WINDOW_SECONDS):
        return

    raise HTTPException(status_code=429, detail="Too many scan requests. Please wait and try again.")


def _scan_filter_for_user_game(game_id: str, user_id: str) -> Dict[str, Any]:
    # Keep legacy compatibility for historical rows that predate user_id tagging.
    return {
        "game_id": game_id,
        "$or": [
            {"user_id": user_id},
            {"user_id": {"$exists": False}},
        ],
    }


def _scan_out_from_doc(doc: Dict[str, Any]) -> ScanResultOut:
    posts = _safe_list(doc.get("posts"))
    comments = _safe_list(doc.get("comments"))
    return ScanResultOut(
        id=str(doc.get("_id") or doc.get("id")),
        created_at=doc.get("created_at"),
        analysis=doc.get("analysis") or {},
        posts_count=len(posts),
        comments_count=len(comments),
        scan_type=(str(doc.get("scan_type") or "").strip() or None),
    )


def _scan_detail_out_from_doc(doc: Dict[str, Any]) -> ScanResultDetailOut:
    return ScanResultDetailOut(
        id=str(doc.get("_id") or doc.get("id")),
        created_at=doc.get("created_at"),
        analysis=doc.get("analysis") or {},
        posts=_safe_list(doc.get("posts")),
        comments=_safe_list(doc.get("comments")),
        scan_type=(str(doc.get("scan_type") or "").strip() or None),
        subreddit_breakdown=(
            doc.get("subreddit_breakdown") if isinstance(doc.get("subreddit_breakdown"), dict) else None
        ),
        meta=(doc.get("meta") if isinstance(doc.get("meta"), dict) else None),
    )


def _doc_created_at(doc: Dict[str, Any]) -> Optional[datetime]:
    value = doc.get("created_at")
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        candidate = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(candidate)
            if parsed.tzinfo is not None:
                return parsed.replace(tzinfo=None)
            return parsed
        except Exception:
            return None
    return None


def _sentiment_score(label: str) -> int:
    value = str(label or "").strip().lower()
    if "positive" in value:
        return 1
    if "negative" in value:
        return -1
    return 0


def _sentiment_direction(delta: int) -> str:
    if delta > 0:
        return "improved"
    if delta < 0:
        return "declined"
    return "unchanged"


def _analysis_list_texts(analysis: Any, key: str, max_items: int = 10) -> List[str]:
    if not isinstance(analysis, dict):
        return []
    raw = analysis.get(key)
    if not isinstance(raw, list):
        return []

    items: List[str] = []
    for entry in raw:
        text_value = ""
        if isinstance(entry, str):
            text_value = entry.strip()
        elif isinstance(entry, dict):
            text_value = str(
                entry.get("text")
                or entry.get("summary")
                or entry.get("point")
                or entry.get("title")
                or ""
            ).strip()
        else:
            text_value = str(entry or "").strip()

        if text_value and text_value not in items:
            items.append(text_value)
        if len(items) >= max(max_items, 1):
            break

    return items


def _normalize_compare_key(text: str, prefer_prefix: bool = False) -> str:
    value = str(text or "")
    if prefer_prefix and " - " in value:
        value = value.split(" - ", 1)[0]

    value = re.sub(r"\[POST:[A-Za-z0-9_]+\]", " ", value)
    value = re.sub(r"https?://\S+", " ", value)
    tokens = [token for token in re.findall(r"[a-z0-9]+", value.lower()) if len(token) >= 3]
    if not tokens:
        return ""
    max_tokens = 8 if prefer_prefix else 16
    return " ".join(tokens[:max_tokens])


def _build_item_change_set(
    from_items: List[str],
    to_items: List[str],
    *,
    prefer_prefix: bool = False,
    max_samples: int = 5,
) -> Dict[str, Any]:
    def _to_map(items: List[str]) -> Dict[str, str]:
        mapped: Dict[str, str] = {}
        for item in items:
            key = _normalize_compare_key(item, prefer_prefix=prefer_prefix)
            if not key:
                continue
            if key not in mapped:
                mapped[key] = item
        return mapped

    from_map = _to_map(from_items)
    to_map = _to_map(to_items)

    new_keys = [key for key in to_map.keys() if key not in from_map]
    removed_keys = [key for key in from_map.keys() if key not in to_map]
    persisting_keys = [key for key in to_map.keys() if key in from_map]

    return {
        "new": [to_map[key] for key in new_keys[:max_samples]],
        "removed": [from_map[key] for key in removed_keys[:max_samples]],
        "persisting": [to_map[key] for key in persisting_keys[:max_samples]],
        "new_count": len(new_keys),
        "removed_count": len(removed_keys),
        "persisting_count": len(persisting_keys),
    }


_THEME_SIGNAL_STOP_WORDS = {
    "the", "and", "with", "from", "this", "that", "have", "your", "about", "into", "they",
    "their", "them", "what", "when", "where", "which", "were", "been", "just", "also", "more",
    "some", "many", "over", "than", "there", "users", "community", "game", "reddit", "player",
    "players", "feel", "feels", "really", "very", "still", "mode", "modes", "thread", "post",
}

_THEME_SIGNAL_BUCKETS: List[Dict[str, Any]] = [
    {
        "key": "pvp_pve_tension",
        "label": "PvP vs PvE Tension",
        "tokens": {"pvp", "pve", "encounter", "encounters", "lobbies"},
        "phrases": ["pvp vs pve", "pve experience", "pvp encounters", "pve-only"],
    },
    {
        "key": "pve_only_requests",
        "label": "PvE-Only Requests",
        "tokens": {"pve", "cooperative", "coop"},
        "phrases": ["pve only", "pve-only", "pve mode", "co-op only", "coop only"],
    },
    {
        "key": "matchmaking_balance",
        "label": "Matchmaking & Lobby Balance",
        "tokens": {"matchmaking", "matched", "mismatch", "lobby", "lobbies", "sweaty"},
        "phrases": ["skill based", "matched against", "aggressive lobbies", "matchmaking"],
    },
    {
        "key": "performance_stability",
        "label": "Performance & Stability",
        "tokens": {"crash", "crashes", "lag", "stutter", "performance", "fps", "freeze", "disconnect"},
        "phrases": ["frame rate", "low fps", "performance issue", "server lag"],
    },
    {
        "key": "queue_times",
        "label": "Queue Times & Match Start Delay",
        "tokens": {"queue", "queues", "queued", "waiting", "delay", "delays"},
        "phrases": ["queue time", "long queue", "match start"],
    },
    {
        "key": "balance_tuning",
        "label": "Balance & Tuning",
        "tokens": {"balance", "balanced", "unbalanced", "nerf", "buff", "meta", "weapon"},
        "phrases": ["too strong", "too weak", "needs nerf", "needs buff"],
    },
    {
        "key": "progression_rewards",
        "label": "Progression & Rewards",
        "tokens": {"progression", "progress", "xp", "reward", "rewards", "unlock", "unlocks", "grind"},
        "phrases": ["battle pass", "too grindy", "rewarding", "progression"],
    },
    {
        "key": "content_variety",
        "label": "Content Variety & Modes",
        "tokens": {"content", "map", "maps", "mission", "missions", "mode", "modes", "variety"},
        "phrases": ["more content", "new map", "new mode", "content drought"],
    },
    {
        "key": "cheaters_exploits",
        "label": "Cheaters & Exploits",
        "tokens": {"cheater", "cheaters", "hack", "hacker", "hackers", "exploit", "exploits"},
        "phrases": ["anti cheat", "anti-cheat", "cheater problem"],
    },
    {
        "key": "social_toxicity",
        "label": "Toxicity & Player Behavior",
        "tokens": {"toxic", "toxicity", "grief", "griefing", "sweats", "sweaty", "camping"},
        "phrases": ["toxic behavior", "toxic players", "player behavior"],
    },
]


def _theme_tokens(value: str) -> List[str]:
    return re.findall(r"[a-z0-9]+", str(value or "").lower())


def _theme_text_tokens(value: str) -> List[str]:
    return [
        token
        for token in _theme_tokens(value)
        if len(token) >= 3 and token not in _THEME_SIGNAL_STOP_WORDS
    ]


def _extract_post_ids_from_text(value: str) -> List[str]:
    ids: List[str] = []
    for match in re.findall(r"\[POST:([A-Za-z0-9_]+)\]", str(value or "")):
        post_id = str(match or "").strip()
        if post_id and post_id not in ids:
            ids.append(post_id)
    return ids


def _post_theme_weight(post: Dict[str, Any]) -> float:
    score = max(0, int(post.get("score", 0) or 0))
    comments = max(0, int(post.get("num_comments", 0) or 0))
    return 1.0 + math.log(score + 1) + 1.15 * math.log(comments + 1)


def _comment_theme_weight(comment: Dict[str, Any]) -> float:
    score = max(0, int(comment.get("score", 0) or 0))
    return 0.35 + 0.6 * math.log(score + 1)


def _theme_bucket_match_strength(bucket: Dict[str, Any], text: str, tokens: List[str]) -> int:
    token_set = set(tokens)
    strength = 0
    for token in bucket.get("tokens", set()):
        clean = str(token or "").lower()
        if clean and clean in token_set:
            strength += 1
    lower_text = str(text or "").lower()
    for phrase in bucket.get("phrases", []):
        candidate = str(phrase or "").lower().strip()
        if candidate and candidate in lower_text:
            strength += 2
    return strength


def _extract_theme_signals(
    posts: List[Dict[str, Any]],
    comments: List[Dict[str, Any]],
    analysis: Optional[Dict[str, Any]] = None,
    max_signals: int = 8,
) -> List[Dict[str, Any]]:
    safe_posts = [post for post in _safe_list(posts) if isinstance(post, dict)]
    safe_comments = [comment for comment in _safe_list(comments) if isinstance(comment, dict)]
    safe_analysis = analysis if isinstance(analysis, dict) else {}

    bucket_rows: Dict[str, Dict[str, Any]] = {}
    for bucket in _THEME_SIGNAL_BUCKETS:
        bucket_rows[str(bucket["key"])] = {
            "key": str(bucket["key"]),
            "label": str(bucket["label"]),
            "score": 0.0,
            "mention_posts": set(),
        }

    # Score posts (title + selftext) into stable buckets.
    for post in safe_posts:
        post_id = str(post.get("id") or "").strip()
        text = f"{post.get('title', '')} {post.get('selftext', '')}"
        tokens = _theme_text_tokens(text)
        if not tokens:
            continue
        weight = _post_theme_weight(post)

        for bucket in _THEME_SIGNAL_BUCKETS:
            strength = _theme_bucket_match_strength(bucket, text, tokens)
            if strength <= 0:
                continue
            row = bucket_rows[str(bucket["key"])]
            row["score"] += weight * (1.0 + (0.15 * max(0, strength - 1)))
            if post_id:
                row["mention_posts"].add(post_id)

    # Score sampled comments into the same buckets with smaller weight.
    for comment in safe_comments:
        body = str(comment.get("body", "") or "")
        tokens = _theme_text_tokens(body)
        if not tokens:
            continue
        weight = _comment_theme_weight(comment)
        source_post_id = str(comment.get("source_post_id") or "").strip()

        for bucket in _THEME_SIGNAL_BUCKETS:
            strength = _theme_bucket_match_strength(bucket, body, tokens)
            if strength <= 0:
                continue
            row = bucket_rows[str(bucket["key"])]
            row["score"] += weight * (0.75 + (0.1 * max(0, strength - 1)))
            if source_post_id:
                row["mention_posts"].add(source_post_id)

    # Use AI-extracted themes as a lightweight boost, but not as the canonical identity.
    for theme_text in _analysis_list_texts(safe_analysis, "themes", max_items=12):
        theme_tokens = _theme_text_tokens(theme_text)
        if not theme_tokens:
            continue
        for bucket in _THEME_SIGNAL_BUCKETS:
            strength = _theme_bucket_match_strength(bucket, theme_text, theme_tokens)
            if strength <= 0:
                continue
            row = bucket_rows[str(bucket["key"])]
            row["score"] += 1.2 + (0.3 * max(0, strength - 1))
            for post_id in _extract_post_ids_from_text(theme_text):
                row["mention_posts"].add(post_id)

    # Add dynamic title phrases for game-specific themes not covered by the static buckets.
    phrase_rows: Dict[str, Dict[str, Any]] = {}
    phrase_stop = _THEME_SIGNAL_STOP_WORDS.union({"pvp", "pve", "matchmaking", "mode", "modes"})

    for post in safe_posts:
        post_id = str(post.get("id") or "").strip()
        title = str(post.get("title", "") or "")
        tokens = [token for token in _theme_tokens(title) if len(token) >= 3 and token not in phrase_stop]
        if len(tokens) < 2:
            continue
        weight = _post_theme_weight(post)
        seen_in_post = set()

        for n in (3, 2):
            if len(tokens) < n:
                continue
            for idx in range(len(tokens) - n + 1):
                phrase_tokens = tokens[idx : idx + n]
                phrase = " ".join(phrase_tokens)
                if phrase in seen_in_post:
                    continue
                seen_in_post.add(phrase)

                key = "title:" + "-".join(phrase_tokens[:4])
                row = phrase_rows.get(key)
                if row is None:
                    row = {
                        "key": key,
                        "label": phrase.title(),
                        "score": 0.0,
                        "mention_posts": set(),
                    }
                    phrase_rows[key] = row
                row["score"] += weight
                if post_id:
                    row["mention_posts"].add(post_id)

    signals: List[Dict[str, Any]] = []

    for row in bucket_rows.values():
        mentions = sorted(list(row.get("mention_posts", set())))
        mention_count = len(mentions)
        score = float(row.get("score", 0.0) or 0.0)
        if mention_count == 0 or score < 2.25:
            continue
        signals.append(
            {
                "key": str(row.get("key") or ""),
                "label": str(row.get("label") or ""),
                "score": round(score, 3),
                "mention_count": mention_count,
                "evidence_post_ids": mentions[:5],
                "kind": "bucket",
            }
        )

    # Keep only stronger dynamic phrases to avoid noisy duplication.
    phrase_candidates = sorted(
        phrase_rows.values(),
        key=lambda item: (float(item.get("score", 0.0) or 0.0), len(item.get("mention_posts", set()))),
        reverse=True,
    )
    added_dynamic = 0
    for row in phrase_candidates:
        if added_dynamic >= 4:
            break
        mentions = sorted(list(row.get("mention_posts", set())))
        mention_count = len(mentions)
        score = float(row.get("score", 0.0) or 0.0)
        if mention_count < 2 or score < 4.0:
            continue

        label_tokens = set(_theme_text_tokens(str(row.get("label") or "")))
        if not label_tokens:
            continue
        overlaps_bucket = False
        for bucket in _THEME_SIGNAL_BUCKETS:
            bucket_tokens = {str(t).lower() for t in bucket.get("tokens", set())}
            if len(label_tokens.intersection(bucket_tokens)) >= 2:
                overlaps_bucket = True
                break
        if overlaps_bucket:
            continue

        signals.append(
            {
                "key": str(row.get("key") or ""),
                "label": str(row.get("label") or ""),
                "score": round(score, 3),
                "mention_count": mention_count,
                "evidence_post_ids": mentions[:5],
                "kind": "title_phrase",
            }
        )
        added_dynamic += 1

    if not signals:
        # Fallback to AI themes but store normalized keys to reduce paraphrase noise later.
        for theme_text in _analysis_list_texts(safe_analysis, "themes", max_items=max_signals):
            key = _normalize_compare_key(theme_text, prefer_prefix=True)
            if not key:
                continue
            if any(str(item.get("key") or "") == key for item in signals):
                continue
            label = str(theme_text).split(" - ", 1)[0].strip() or str(theme_text).strip()
            signals.append(
                {
                    "key": key,
                    "label": label[:120],
                    "score": 1.0,
                    "mention_count": len(_extract_post_ids_from_text(theme_text)),
                    "evidence_post_ids": _extract_post_ids_from_text(theme_text)[:5],
                    "kind": "ai_fallback",
                }
            )
            if len(signals) >= max(max_signals, 1):
                break

    signals.sort(
        key=lambda item: (
            float(item.get("score", 0.0) or 0.0),
            int(item.get("mention_count", 0) or 0),
            str(item.get("label") or ""),
        ),
        reverse=True,
    )
    return [dict(item) for item in signals[: max(max_signals, 1)]]


def _theme_signal_rows_from_doc(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = doc.get("theme_signals")
    if isinstance(raw, list):
        rows: List[Dict[str, Any]] = []
        for item in raw:
            if not isinstance(item, dict):
                continue
            key = str(item.get("key") or "").strip()
            label = str(item.get("label") or "").strip()
            if not key or not label:
                continue
            evidence_post_ids = [
                str(v).strip()
                for v in (item.get("evidence_post_ids") or [])
                if str(v).strip()
            ][:5]
            rows.append(
                {
                    "key": key,
                    "label": label,
                    "score": float(item.get("score", 0.0) or 0.0),
                    "mention_count": int(item.get("mention_count", 0) or 0),
                    "evidence_post_ids": evidence_post_ids,
                    "kind": str(item.get("kind") or ""),
                }
            )
        if rows:
            return rows

    return _extract_theme_signals(
        _safe_list(doc.get("posts")),
        _safe_list(doc.get("comments")),
        doc.get("analysis") if isinstance(doc.get("analysis"), dict) else {},
    )


def _format_theme_signal_change_row(signal: Dict[str, Any], other: Optional[Dict[str, Any]] = None) -> str:
    label = str(signal.get("label") or signal.get("key") or "Theme").strip()
    score = round(float(signal.get("score", 0.0) or 0.0), 1)
    mentions = int(signal.get("mention_count", 0) or 0)
    refs = [str(v).strip() for v in (signal.get("evidence_post_ids") or []) if str(v).strip()]
    ref_text = f" [POST:{refs[0]}]" if refs else ""

    if other and isinstance(other, dict):
        prev_score = round(float(other.get("score", 0.0) or 0.0), 1)
        prev_mentions = int(other.get("mention_count", 0) or 0)
        score_delta = round(score - prev_score, 1)
        mentions_delta = mentions - prev_mentions
        return (
            f"{label} - signal {score_delta:+.1f} (now {score:.1f}), mentions {mentions_delta:+d} (now {mentions}){ref_text}"
        )

    return f"{label} - signal {score:.1f}, mentions {mentions}{ref_text}"


def _build_theme_signal_change_set(from_doc: Dict[str, Any], to_doc: Dict[str, Any], max_samples: int = 5) -> Dict[str, Any]:
    from_rows = _theme_signal_rows_from_doc(from_doc)
    to_rows = _theme_signal_rows_from_doc(to_doc)

    from_map = {str(item.get("key") or ""): item for item in from_rows if str(item.get("key") or "")}
    to_map = {str(item.get("key") or ""): item for item in to_rows if str(item.get("key") or "")}

    rising_or_new: List[str] = []
    lowered_or_removed: List[str] = []
    persisting: List[str] = []
    new_count = 0
    removed_count = 0
    rising_count = 0
    falling_count = 0
    stable_count = 0

    for key, to_signal in to_map.items():
        from_signal = from_map.get(key)
        if not from_signal:
            new_count += 1
            rising_or_new.append(_format_theme_signal_change_row(to_signal))
            continue

        to_score = float(to_signal.get("score", 0.0) or 0.0)
        from_score = float(from_signal.get("score", 0.0) or 0.0)
        to_mentions = int(to_signal.get("mention_count", 0) or 0)
        from_mentions = int(from_signal.get("mention_count", 0) or 0)
        score_delta = to_score - from_score
        mentions_delta = to_mentions - from_mentions

        if score_delta >= 1.0 or (score_delta >= 0.5 and mentions_delta > 0):
            rising_count += 1
            rising_or_new.append(_format_theme_signal_change_row(to_signal, from_signal))
        elif score_delta <= -1.0 or (score_delta <= -0.5 and mentions_delta < 0):
            falling_count += 1
            lowered_or_removed.append(_format_theme_signal_change_row(from_signal, to_signal))
        else:
            stable_count += 1
            persisting.append(_format_theme_signal_change_row(to_signal, from_signal))

    for key, from_signal in from_map.items():
        if key in to_map:
            continue
        removed_count += 1
        lowered_or_removed.append(_format_theme_signal_change_row(from_signal))

    return {
        "new": rising_or_new[:max_samples],
        "removed": lowered_or_removed[:max_samples],
        "persisting": persisting[:max_samples],
        "new_count": new_count + rising_count,
        "removed_count": removed_count + falling_count,
        "persisting_count": stable_count,
        "signal_based": True,
        "raw_counts": {
            "new": new_count,
            "removed": removed_count,
            "rising": rising_count,
            "falling": falling_count,
            "stable": stable_count,
        },
        "from_signals": [
            {
                "key": item.get("key"),
                "label": item.get("label"),
                "score": item.get("score"),
                "mention_count": item.get("mention_count"),
                "evidence_post_ids": item.get("evidence_post_ids") or [],
                "kind": item.get("kind") or "",
            }
            for item in from_rows[:10]
        ],
        "to_signals": [
            {
                "key": item.get("key"),
                "label": item.get("label"),
                "score": item.get("score"),
                "mention_count": item.get("mention_count"),
                "evidence_post_ids": item.get("evidence_post_ids") or [],
                "kind": item.get("kind") or "",
            }
            for item in to_rows[:10]
        ],
    }


def _subreddit_sentiment_map(doc: Dict[str, Any]) -> Dict[str, str]:
    breakdown = doc.get("subreddit_breakdown")
    if not isinstance(breakdown, dict):
        return {}
    rows = breakdown.get("breakdown")
    if not isinstance(rows, list):
        return {}

    result: Dict[str, str] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        subreddit = str(row.get("subreddit") or "").strip().lower()
        if not subreddit:
            continue
        result[subreddit] = str(row.get("sentiment_label") or "Unknown")
    return result


def _build_subreddit_sentiment_changes(from_doc: Dict[str, Any], to_doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    from_map = _subreddit_sentiment_map(from_doc)
    to_map = _subreddit_sentiment_map(to_doc)
    if not from_map and not to_map:
        return []

    rows: List[Dict[str, Any]] = []
    for subreddit in sorted(set(from_map.keys()) | set(to_map.keys())):
        from_label = from_map.get(subreddit, "Not present")
        to_label = to_map.get(subreddit, "Not present")
        delta = _sentiment_score(to_label) - _sentiment_score(from_label)
        if subreddit in from_map and subreddit in to_map and from_label == to_label:
            continue

        rows.append(
            {
                "subreddit": subreddit,
                "from_sentiment": from_label,
                "to_sentiment": to_label,
                "delta": delta,
                "direction": _sentiment_direction(delta),
            }
        )

    rows.sort(key=lambda item: (-abs(int(item.get("delta", 0) or 0)), str(item.get("subreddit") or "")))
    return rows[:10]


def _build_compare_payload(from_doc: Dict[str, Any], to_doc: Dict[str, Any]) -> ScanCompareOut:
    from_out = _scan_out_from_doc(from_doc)
    to_out = _scan_out_from_doc(to_doc)

    from_analysis = from_doc.get("analysis") if isinstance(from_doc.get("analysis"), dict) else {}
    to_analysis = to_doc.get("analysis") if isinstance(to_doc.get("analysis"), dict) else {}

    sentiment_from = str(from_analysis.get("sentiment_label") or "Unknown")
    sentiment_to = str(to_analysis.get("sentiment_label") or "Unknown")
    sentiment_delta = _sentiment_score(sentiment_to) - _sentiment_score(sentiment_from)

    posts_delta = int(to_out.posts_count or 0) - int(from_out.posts_count or 0)
    comments_delta = int(to_out.comments_count or 0) - int(from_out.comments_count or 0)

    from_created_at = _doc_created_at(from_doc)
    to_created_at = _doc_created_at(to_doc)
    days_between = 0.0
    if from_created_at and to_created_at:
        days_between = round(max((to_created_at - from_created_at).total_seconds(), 0.0) / 86400.0, 2)

    theme_changes = _build_theme_signal_change_set(from_doc, to_doc)
    pain_point_changes = _build_item_change_set(
        _analysis_list_texts(from_analysis, "pain_points", max_items=8),
        _analysis_list_texts(to_analysis, "pain_points", max_items=8),
    )
    win_changes = _build_item_change_set(
        _analysis_list_texts(from_analysis, "wins", max_items=8),
        _analysis_list_texts(to_analysis, "wins", max_items=8),
    )

    return ScanCompareOut(
        from_result=from_out,
        to_result=to_out,
        sentiment_from=sentiment_from,
        sentiment_to=sentiment_to,
        sentiment_score_delta=sentiment_delta,
        posts_delta=posts_delta,
        comments_delta=comments_delta,
        summary={
            "direction": _sentiment_direction(sentiment_delta),
            "days_between": days_between,
            "scan_type_from": from_out.scan_type or "single",
            "scan_type_to": to_out.scan_type or "single",
            "theme_new_count": int(theme_changes.get("new_count", 0) or 0),
            "theme_removed_count": int(theme_changes.get("removed_count", 0) or 0),
            "pain_new_count": int(pain_point_changes.get("new_count", 0) or 0),
            "pain_removed_count": int(pain_point_changes.get("removed_count", 0) or 0),
            "win_new_count": int(win_changes.get("new_count", 0) or 0),
            "win_removed_count": int(win_changes.get("removed_count", 0) or 0),
        },
        theme_changes=theme_changes,
        pain_point_changes=pain_point_changes,
        win_changes=win_changes,
        subreddit_sentiment_changes=_build_subreddit_sentiment_changes(from_doc, to_doc),
    )


def _parse_window_days(window: str) -> int:
    value = str(window or "30d").strip().lower()
    match = re.fullmatch(r"(\d{1,3})d", value)
    if not match:
        raise HTTPException(status_code=400, detail="Invalid window. Use formats like 7d, 30d, 90d.")

    days = int(match.group(1))
    if days < 1 or days > 365:
        raise HTTPException(status_code=400, detail="Window must be between 1d and 365d.")
    return days


@router.post("/multi-scan")
async def run_multi_scan(payload: MultiScanRequest, request: Request, user=Depends(get_current_user)):
    _enforce_scan_rate_limit(request, user["user_id"], scope="multi")

    if not payload.subreddits:
        raise HTTPException(status_code=400, detail="At least one subreddit is required")

    try:
        result = await services.scan_multiple_subreddits(
            subreddits=payload.subreddits,
            game_name=str(payload.game_name or ""),
            keywords=str(payload.keywords or ""),
            include_breakdown=bool(payload.include_breakdown),
            include_internal=bool(str(payload.game_id or "").strip()),
        )
    except RuntimeError as exc:
        detail = str(exc)
        lowered = detail.lower()
        if "at least one valid subreddit" in lowered:
            raise HTTPException(status_code=400, detail=detail)
        if "no posts found" in lowered:
            raise HTTPException(status_code=404, detail=detail)
        raise HTTPException(status_code=500, detail=detail)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Multi scan failed: {exc}")

    game_id = str(payload.game_id or "").strip()
    if game_id:
        game = await database.db.tracked_games.find_one({"_id": game_id, "user_id": user["user_id"]})
        if not game:
            raise HTTPException(status_code=404, detail="Game not found")

        multi_posts = _safe_list(result.get("_posts"))
        multi_comments = _safe_list(result.get("_comments"))
        overall_analysis = result.get("overall") if isinstance(result.get("overall"), dict) else {}

        scan_doc = {
            "_id": str(uuid.uuid4()),
            "game_id": game_id,
            "user_id": user["user_id"],
            "created_at": datetime.utcnow(),
            "posts": multi_posts,
            "comments": multi_comments,
            "analysis": overall_analysis,
            "scan_type": "multi",
            "subreddit_breakdown": result.get("subreddit_breakdown") or {"breakdown": []},
            "meta": result.get("meta") or {},
            "theme_signals": _extract_theme_signals(multi_posts, multi_comments, overall_analysis),
        }
        await database.db.scan_results.insert_one(scan_doc)

    return {
        "overall": result.get("overall") or {},
        "meta": result.get("meta") or {},
        "subreddit_breakdown": result.get("subreddit_breakdown") or {"breakdown": []},
    }


@router.post("/{id}/scan")
async def run_scan(id: str, request: Request, user=Depends(get_current_user)):
    _enforce_scan_rate_limit(request, user["user_id"], scope="single")

    game = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    subreddit = game.get("subreddit", "")

    try:
        posts = await services.fetch_reddit_posts(subreddit, limit=100)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Failed to fetch Reddit posts: {exc}")

    if not posts:
        raise HTTPException(
            status_code=404,
            detail=f"No posts found for r/{subreddit}. Check subreddit name or try again later.",
        )

    try:
        comments = await services.sample_comments_for_posts(
            posts,
            max_posts=services.TOP_POSTS_FOR_COMMENTS,
            max_comments_per_post=services.MAX_COMMENTS_PER_POST,
        )
    except Exception:
        comments = []

    try:
        analysis = await services.analyze_posts_with_ai(
            posts,
            comments,
            game_name=str(game.get("name", "") or ""),
            keywords=str(game.get("keywords", "") or ""),
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"AI analysis failed: {exc}")

    result = {
        "_id": str(uuid.uuid4()),
        "game_id": id,
        "user_id": user["user_id"],
        "created_at": datetime.utcnow(),
        "posts": posts,
        "comments": comments,
        "analysis": analysis,
        "scan_type": "single",
        "theme_signals": _extract_theme_signals(posts, comments, analysis),
    }

    await database.db.scan_results.insert_one(result)
    return {"message": "scan complete", "result_id": result["_id"]}


@router.get("/{id}/results", response_model=List[ScanResultOut])
async def list_results(id: str, user=Depends(get_current_user)):
    game = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    cursor = database.db.scan_results.find(_scan_filter_for_user_game(id, user["user_id"])).sort("created_at", -1)
    results: List[ScanResultOut] = []
    async for r in cursor:
        results.append(_scan_out_from_doc(r))
    return results


@router.get("/{id}/results/compare", response_model=ScanCompareOut)
async def compare_results(
    id: str,
    from_result_id: Optional[str] = None,
    to_result_id: Optional[str] = None,
    user=Depends(get_current_user),
):
    game = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    cursor = (
        database.db.scan_results
        .find(_scan_filter_for_user_game(id, user["user_id"]))
        .sort("created_at", -1)
        .limit(250)
    )
    docs: List[Dict[str, Any]] = []
    async for row in cursor:
        docs.append(row)

    if len(docs) < 2:
        raise HTTPException(status_code=404, detail="Need at least two scans to compare")

    by_id = {str(doc.get("_id") or doc.get("id")): doc for doc in docs}

    if to_result_id:
        to_key = str(to_result_id).strip()
        to_doc = by_id.get(to_key)
        if not to_doc:
            raise HTTPException(status_code=404, detail="Target scan not found")
        try:
            to_idx = next(idx for idx, doc in enumerate(docs) if str(doc.get("_id") or doc.get("id")) == to_key)
        except StopIteration:
            raise HTTPException(status_code=404, detail="Target scan not found")
    else:
        to_doc = docs[0]
        to_idx = 0

    if from_result_id:
        from_key = str(from_result_id).strip()
        from_doc = by_id.get(from_key)
        if not from_doc:
            raise HTTPException(status_code=404, detail="Baseline scan not found")
    else:
        if to_idx + 1 >= len(docs):
            raise HTTPException(status_code=404, detail="No previous scan available for comparison")
        from_doc = docs[to_idx + 1]

    if str(from_doc.get("_id") or from_doc.get("id")) == str(to_doc.get("_id") or to_doc.get("id")):
        raise HTTPException(status_code=400, detail="Cannot compare a scan to itself")

    from_dt = _doc_created_at(from_doc)
    to_dt = _doc_created_at(to_doc)
    if from_dt and to_dt and from_dt > to_dt:
        from_doc, to_doc = to_doc, from_doc

    return _build_compare_payload(from_doc, to_doc)


@router.get("/{id}/results/trends", response_model=ScanTrendsOut)
async def scan_trends(
    id: str,
    window: str = "30d",
    limit: int = 120,
    user=Depends(get_current_user),
):
    game = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    days = _parse_window_days(window)
    safe_limit = max(1, min(int(limit or 120), 500))
    cutoff = datetime.utcnow() - timedelta(days=days)

    query = _scan_filter_for_user_game(id, user["user_id"])
    query["created_at"] = {"$gte": cutoff}

    cursor = database.db.scan_results.find(query).sort("created_at", -1).limit(safe_limit)
    docs_desc: List[Dict[str, Any]] = []
    async for row in cursor:
        docs_desc.append(row)

    docs_asc = list(reversed(docs_desc))
    points: List[ScanTrendPointOut] = []
    sentiment_counts = {"Positive": 0, "Mixed": 0, "Negative": 0, "Unknown": 0}
    total_posts = 0
    total_comments = 0

    for doc in docs_asc:
        out = _scan_out_from_doc(doc)
        label = str((doc.get("analysis") or {}).get("sentiment_label") or "Unknown")
        score = _sentiment_score(label)
        normalized_label = "Positive" if score > 0 else "Negative" if score < 0 else ("Mixed" if "mixed" in label.lower() else "Unknown")
        sentiment_counts[normalized_label] = int(sentiment_counts.get(normalized_label, 0) or 0) + 1

        posts_count = int(out.posts_count or 0)
        comments_count = int(out.comments_count or 0)
        total_posts += posts_count
        total_comments += comments_count

        points.append(
            ScanTrendPointOut(
                id=out.id,
                created_at=out.created_at,
                sentiment_label=label,
                sentiment_score=score,
                posts_count=posts_count,
                comments_count=comments_count,
                scan_type=out.scan_type,
            )
        )

    summary: Dict[str, Any] = {
        "window_days": days,
        "sentiment_counts": sentiment_counts,
        "total_posts": total_posts,
        "total_comments": total_comments,
        "average_posts_per_scan": round(total_posts / len(points), 2) if points else 0.0,
        "average_comments_per_scan": round(total_comments / len(points), 2) if points else 0.0,
        "latest_sentiment": points[-1].sentiment_label if points else "Unknown",
        "oldest_sentiment": points[0].sentiment_label if points else "Unknown",
    }

    if len(docs_desc) >= 2:
        latest_vs_previous = _build_compare_payload(docs_desc[1], docs_desc[0])
        summary["latest_vs_previous"] = {
            "from_result_id": latest_vs_previous.from_result.id,
            "to_result_id": latest_vs_previous.to_result.id,
            "direction": latest_vs_previous.summary.get("direction"),
            "sentiment_from": latest_vs_previous.sentiment_from,
            "sentiment_to": latest_vs_previous.sentiment_to,
            "sentiment_score_delta": latest_vs_previous.sentiment_score_delta,
            "posts_delta": latest_vs_previous.posts_delta,
            "comments_delta": latest_vs_previous.comments_delta,
        }

    return ScanTrendsOut(window=f"{days}d", scan_count=len(points), points=points, summary=summary)


@router.get("/{id}/results/{result_id}/detail", response_model=ScanResultDetailOut)
async def result_detail(id: str, result_id: str, user=Depends(get_current_user)):
    game = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    query = _scan_filter_for_user_game(id, user["user_id"])
    query["_id"] = result_id
    r = await database.db.scan_results.find_one(query)
    if not r:
        raise HTTPException(status_code=404, detail="Scan result not found")

    return _scan_detail_out_from_doc(r)


@router.get("/{id}/latest-result", response_model=ScanResultOut)
async def latest_result(id: str, user=Depends(get_current_user)):
    game = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    r = await database.db.scan_results.find_one(
        _scan_filter_for_user_game(id, user["user_id"]),
        sort=[("created_at", -1)],
    )
    if not r:
        raise HTTPException(status_code=404)
    return _scan_out_from_doc(r)


@router.get("/{id}/latest-result-detail", response_model=ScanResultDetailOut)
async def latest_result_detail(id: str, user=Depends(get_current_user)):
    game = await database.db.tracked_games.find_one({"_id": id, "user_id": user["user_id"]})
    if not game:
        raise HTTPException(status_code=404, detail="Game not found")

    r = await database.db.scan_results.find_one(
        _scan_filter_for_user_game(id, user["user_id"]),
        sort=[("created_at", -1)],
    )
    if not r:
        raise HTTPException(status_code=404, detail="No scan results yet")
    return _scan_detail_out_from_doc(r)
