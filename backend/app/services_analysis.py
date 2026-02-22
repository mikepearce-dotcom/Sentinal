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

EXECUTIVE SUMMARY REQUIREMENTS (1-2 paragraphs, 5-9 sentences total):
- Write in an executive style: clear diagnosis, evidence, and product implications.
- Paragraph 1: overall sentiment, primary drivers, and confidence based on repeated signals.
- Paragraph 2: key risks, strongest positives, and what should be prioritized next.
- Include at least three [POST:post_id] references inside sentiment_summary.
- Keep it specific and evidence-led; avoid generic statements.

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
2. sentiment_summary: 1-2 paragraph executive summary with required structure
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

def _summary_word_count(text: str) -> int:
    return len(re.findall(r"[A-Za-z0-9']+", str(text or "")))

def _summary_post_ref_count(text: str) -> int:
    post_ids = _extract_post_ids(str(text or ""))
    return len(set(post_ids))

def _summary_sentence_count(text: str) -> int:
    value = str(text or "").strip()
    if not value:
        return 0
    return len([part for part in re.split(r"(?<=[.!?])\s+", value) if str(part).strip()])

def _should_refine_executive_summary(summary_text: str, posts: List[Dict[str, Any]]) -> bool:
    if len(posts or []) < 8:
        return False
    words = _summary_word_count(summary_text)
    refs = _summary_post_ref_count(summary_text)
    sentences = _summary_sentence_count(summary_text)
    return words < 120 or refs < 3 or sentences < 5

def _ensure_detailed_sentiment_summary(primary_summary: str, fallback_summary: str) -> str:
    primary = str(primary_summary or "").strip()
    fallback = str(fallback_summary or "").strip()
    if not primary:
        return fallback
    has_depth = _summary_word_count(primary) >= 65 and _summary_post_ref_count(primary) >= 2
    if has_depth:
        return primary
    if fallback and fallback not in primary:
        if _summary_word_count(primary) < 55 or _summary_post_ref_count(primary) < 2:
            return f"{primary}\n\n{fallback}".strip()
    return primary

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
    ref_three = refs[2] if len(refs) > 2 else ref_two
    reference_posts = [ref for ref in [ref_one, ref_two, ref_three] if ref]
    if reference_posts:
        reference_block = ", ".join([f"[POST:{ref}]" for ref in reference_posts])
    else:
        reference_block = "the highest-engagement sampled threads"
    primary_ref_label = f"[POST:{ref_one}]" if ref_one else "top-ranked discussion threads"
    secondary_ref_label = f"[POST:{ref_two}]" if ref_two else "secondary high-signal threads"
    tertiary_ref_label = f"[POST:{ref_three}]" if ref_three else "additional corroborating threads"
    sentiment_summary = (
        f"Executive summary: Overall sentiment is {sentiment_label.lower()} across {len(top_posts)} high-signal Reddit threads. "
        f"The dominant discussion drivers are persistent product issues and repeat strengths rather than one-off reactions, with representative evidence in {reference_block}. "
        f"Signal consistency across upvoted and highly-commented posts suggests these themes are materially influencing player experience.\n\n"
        f"From a product perspective, priority should go to the most repeated friction patterns first, while protecting the features players consistently praise for retention and satisfaction. "
        f"The strongest diagnostic references are concentrated around {primary_ref_label} and {secondary_ref_label}, with additional corroboration in {tertiary_ref_label}."
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

    sentiment_summary = _ensure_detailed_sentiment_summary(
        str(normalized.get("sentiment_summary", "") or "").strip(),
        str(fallback.get("sentiment_summary", "") or "").strip(),
    )

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


def _build_executive_summary_refinement_prompt(
    posts: List[Dict[str, Any]],
    comments: List[Dict[str, Any]],
    analysis: Dict[str, Any],
    game_name: str,
    keywords: str,
) -> str:
    top_posts = sorted(posts or [], key=_calculate_post_rank, reverse=True)[:12]
    top_comments = sorted(
        comments or [],
        key=lambda c: (
            int(c.get("score", 0) or 0),
            len(str(c.get("body", "") or "")),
            float(c.get("created_utc", 0) or 0),
        ),
        reverse=True,
    )[:12]

    post_lines: List[str] = []
    for post in top_posts:
        post_id = str(post.get("id") or "").strip()
        title = str(post.get("title", "") or "").strip()
        score = int(post.get("score", 0) or 0)
        num_comments = int(post.get("num_comments", 0) or 0)
        post_lines.append(f"- [POST:{post_id}] [{score} pts, {num_comments} comments] {title}")
        selftext = str(post.get("selftext", "") or "").replace("\n", " ").strip()
        if selftext and selftext not in ("[removed]", "[deleted]"):
            post_lines.append(f"  Snippet: {selftext[:220]}")

    comment_lines: List[str] = []
    for comment in top_comments:
        source_post = str(comment.get("source_post_id") or "").strip()
        score = int(comment.get("score", 0) or 0)
        body = str(comment.get("body", "") or "").replace("\n", " ").strip()
        if not body:
            continue
        comment_lines.append(f"- [POST:{source_post}] [{score} pts] {body[:220]}")

    pain_texts = [
        str(item.get("text", "") or "").strip()
        for item in (analysis.get("pain_points") or [])
        if isinstance(item, dict) and str(item.get("text", "") or "").strip()
    ][:3]
    win_texts = [
        str(item.get("text", "") or "").strip()
        for item in (analysis.get("wins") or [])
        if isinstance(item, dict) and str(item.get("text", "") or "").strip()
    ][:3]
    themes = [str(t).strip() for t in (analysis.get("themes") or []) if str(t).strip()][:5]

    keyword_note = f"\nKeywords to emphasize if supported by evidence: {keywords}" if keywords else ""
    comments_block = "\n".join(comment_lines) if comment_lines else "(no comment samples available)"
    posts_block = "\n".join(post_lines) if post_lines else "(no post samples available)"
    themes_block = "\n".join([f"- {theme}" for theme in themes]) if themes else "(no themes available)"
    pain_block = "\n".join([f"- {item}" for item in pain_texts]) if pain_texts else "(no pain points available)"
    wins_block = "\n".join([f"- {item}" for item in win_texts]) if win_texts else "(no wins available)"

    return f'''Rewrite ONLY the executive summary for a Reddit game feedback analysis.

Game: {game_name or 'Unknown Game'}
Current sentiment label: {analysis.get('sentiment_label', 'Mixed')}
Current summary (too short or not detailed enough):
{str(analysis.get('sentiment_summary', '') or '').strip()}

Use the evidence below and keep the same factual meaning. Do not invent features, platforms, modes, or mechanics.
{keyword_note}

OUTPUT JSON ONLY:
{{"sentiment_summary":"..."}}

SUMMARY REQUIREMENTS:
- 2 paragraphs preferred (1 paragraph only if evidence is genuinely sparse)
- 8-12 sentences total
- ~160-320 words target
- Executive tone: diagnosis, evidence, implications, priorities
- Paragraph 1: overall sentiment, what is driving it, confidence, and evidence breadth
- Paragraph 2: top risks, strongest positives, and what product team should prioritize next
- Include at least 4-6 [POST:post_id] references
- Be specific and evidence-led (avoid generic wording)
- Do not include markdown fences

EXISTING THEMES:
{themes_block}

EXISTING PAIN POINTS:
{pain_block}

EXISTING WINS:
{wins_block}

TOP POSTS:
{posts_block}

TOP COMMENTS:
{comments_block}
'''


async def _refine_executive_summary_with_ai(
    posts: List[Dict[str, Any]],
    comments: List[Dict[str, Any]],
    analysis: Dict[str, Any],
    game_name: str = "",
    keywords: str = "",
    force: bool = False,
) -> Optional[str]:
    current_summary = str(analysis.get("sentiment_summary", "") or "").strip()
    if len(posts or []) < 8:
        return None
    if not force and not _should_refine_executive_summary(current_summary, posts):
        return None

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return None

    try:
        import openai

        openai.api_key = api_key
        prompt = _build_executive_summary_refinement_prompt(
            posts=posts,
            comments=comments,
            analysis=analysis,
            game_name=game_name,
            keywords=keywords,
        )

        response = await openai.ChatCompletion.acreate(
            model="gpt-4o-mini",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You rewrite executive summaries for product insights. "
                        "Return valid JSON only."
                    ),
                },
                {"role": "user", "content": prompt},
            ],
            temperature=0.15,
            max_tokens=550,
        )

        raw_text = response.choices[0].message.content or ""
        parsed = _extract_json_payload(raw_text)
        if not isinstance(parsed, dict):
            return None

        summary = str(parsed.get("sentiment_summary", "") or "").strip()
        if not summary:
            return None

        current_quality = (
            _summary_sentence_count(current_summary),
            _summary_post_ref_count(current_summary),
            _summary_word_count(current_summary),
        )
        candidate_quality = (
            _summary_sentence_count(summary),
            _summary_post_ref_count(summary),
            _summary_word_count(summary),
        )
        if candidate_quality < current_quality:
            return None

        return summary
    except Exception as exc:
        print(f"Executive summary refinement failed: {exc}")
        return None


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
    refine_executive_summary: bool = True,
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

        normalized = ensure_valid_analysis_schema(parsed, posts, game_name=game_name)
        refined_summary: Optional[str] = None
        if refine_executive_summary:
            refined_summary = await _refine_executive_summary_with_ai(
                posts=posts,
                comments=comments,
                analysis=normalized,
                game_name=game_name,
                keywords=keywords,
                force=True,
            )
        if refined_summary:
            normalized["sentiment_summary"] = _ensure_detailed_sentiment_summary(
                refined_summary,
                str(normalized.get("sentiment_summary", "") or "").strip(),
            )

        return normalized
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
    return await analyze_posts_with_ai(
        posts,
        comments,
        game_name=scoped_label,
        keywords=keywords,
        refine_executive_summary=False,
    )


def _extract_post_ids(text: str) -> List[str]:
    return re.findall(r"\[POST:([A-Za-z0-9_]+)\]", text or "")
