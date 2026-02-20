import math
import os
import re
import time
from typing import Any, Dict, List, Optional

from .services_common import (
    MAX_COMMENTS_PER_POST,
    MAX_POSTS_FINAL,
    POST_SELFTEXT_TRUNCATE,
    TOP_POSTS_FOR_COMMENTS,
    _calculate_post_rank,
    _extract_json_payload,
    _format_permalink,
    _normalize_subreddit,
)

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


def _extract_post_ids(text: str) -> List[str]:
    return re.findall(r"\[POST:([A-Za-z0-9_]+)\]", text or "")


