import asyncio
import uuid
from datetime import datetime, timedelta

from app import database, services


def _run_async(coro):
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(coro)
    finally:
        asyncio.set_event_loop(None)
        loop.close()


def test_scan_endpoint(client, monkeypatch):
    # create user and login
    client.post("/api/auth/signup", json={"email": "scan@example.com", "name": "Scanner", "password": "pass"})
    resp = client.post("/api/auth/login", json={"email": "scan@example.com", "password": "pass"})
    token = resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    async def fake_fetch_posts(subreddit, limit=100):
        return [{"id": "1", "title": "post1", "score": 10, "num_comments": 2, "selftext": "content"}]

    async def fake_sample_comments(posts, max_posts=15, max_comments_per_post=10):
        return [{"body": "comment", "source_post_id": "1"}]

    async def fake_analyze(posts, comments, game_name="", keywords=""):
        return {"sentiment_label": "Positive", "themes": [], "pain_points": [], "wins": []}

    monkeypatch.setattr(services, "fetch_reddit_posts", fake_fetch_posts)
    monkeypatch.setattr(services, "sample_comments_for_posts", fake_sample_comments)
    monkeypatch.setattr(services, "analyze_posts_with_ai", fake_analyze)

    # add a game
    r = client.post("/api/games", json={"name": "ScanGame", "subreddit": "scan"}, headers=headers)
    gid = r.json()["id"]

    # run scan
    r = client.post(f"/api/games/{gid}/scan", headers=headers)
    assert r.status_code == 200
    data = r.json()
    assert data.get("message") == "scan complete"
    assert "result_id" in data

    me = client.get("/api/auth/me", headers=headers).json()
    stored = _run_async(database.db.scan_results.find_one({"_id": data["result_id"]}))
    assert stored is not None
    assert stored.get("user_id") == me["user_id"]

    # verify results stored
    r = client.get(f"/api/games/{gid}/latest-result", headers=headers)
    assert r.status_code == 200
    assert r.json()["analysis"]["sentiment_label"] == "Positive"


def test_multi_scan_endpoint(client, monkeypatch):
    client.post("/api/auth/signup", json={"email": "multi@example.com", "name": "Multi", "password": "pass"})
    resp = client.post("/api/auth/login", json={"email": "multi@example.com", "password": "pass"})
    token = resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    async def fake_multi_scan(
        subreddits,
        game_name="",
        keywords="",
        include_breakdown=True,
        include_internal=False,
    ):
        assert subreddits == ["arcraiders", "pcgaming"]
        assert game_name == "Arc Raiders"
        assert include_breakdown is True
        assert include_internal is False
        return {
            "overall": {
                "sentiment_label": "Mixed",
                "sentiment_summary": "Mixed sentiment from combined communities.",
                "themes": ["Matchmaking quality"],
                "pain_points": [{"text": "Queue frustration", "evidence": []}],
                "wins": [{"text": "Core gameplay praised", "evidence": []}],
            },
            "meta": {
                "subreddits": ["arcraiders", "pcgaming"],
                "posts_analysed": 120,
                "comments_sampled": 36,
                "last_scanned": "2026-02-19T00:00:00Z",
            },
            "subreddit_breakdown": {
                "breakdown": [
                    {
                        "subreddit": "arcraiders",
                        "sentiment_label": "Mixed",
                        "summary_bullets": ["Good core loop", "Frustrating queue times"],
                        "top_themes": ["Theme - detail [POST:abc123]"],
                        "top_pain_points": [{"text": "Queue spikes", "evidence": ["https://www.reddit.com/comments/abc123/"]}],
                        "top_wins": [{"text": "Fun combat", "evidence": ["https://www.reddit.com/comments/def456/"]}],
                    }
                ]
            },
        }

    monkeypatch.setattr(services, "scan_multiple_subreddits", fake_multi_scan)

    payload = {
        "subreddits": ["arcraiders", "pcgaming"],
        "game_name": "Arc Raiders",
        "keywords": "queue, extraction",
        "include_breakdown": True,
    }
    r = client.post("/api/games/multi-scan", json=payload, headers=headers)

    assert r.status_code == 200
    body = r.json()
    assert "overall" in body
    assert "meta" in body
    assert "subreddit_breakdown" in body
    assert body["meta"]["posts_analysed"] == 120


def test_multi_scan_rejects_more_than_five_subreddits(client):
    client.post("/api/auth/signup", json={"email": "multi2@example.com", "name": "Multi2", "password": "pass"})
    resp = client.post("/api/auth/login", json={"email": "multi2@example.com", "password": "pass"})
    token = resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    payload = {
        "subreddits": ["a", "b", "c", "d", "e", "f"],
        "game_name": "Arc Raiders",
    }
    r = client.post("/api/games/multi-scan", json=payload, headers=headers)
    assert r.status_code == 422


def test_latest_result_is_user_scoped(client, monkeypatch):
    client.post("/api/auth/signup", json={"email": "owner@example.com", "name": "Owner", "password": "pass"})
    owner_login = client.post("/api/auth/login", json={"email": "owner@example.com", "password": "pass"})
    owner_headers = {"Authorization": f"Bearer {owner_login.json()['access_token']}"}
    owner_me = client.get("/api/auth/me", headers=owner_headers).json()

    client.post("/api/auth/signup", json={"email": "other@example.com", "name": "Other", "password": "pass"})
    other_login = client.post("/api/auth/login", json={"email": "other@example.com", "password": "pass"})
    other_headers = {"Authorization": f"Bearer {other_login.json()['access_token']}"}
    other_me = client.get("/api/auth/me", headers=other_headers).json()

    async def fake_fetch_posts(subreddit, limit=100):
        return [{"id": "1", "title": "owner-post", "score": 10, "num_comments": 2, "selftext": "content"}]

    async def fake_sample_comments(posts, max_posts=15, max_comments_per_post=10):
        return [{"body": "comment", "source_post_id": "1"}]

    async def fake_analyze(posts, comments, game_name="", keywords=""):
        return {"sentiment_label": "Positive", "themes": [], "pain_points": [], "wins": []}

    monkeypatch.setattr(services, "fetch_reddit_posts", fake_fetch_posts)
    monkeypatch.setattr(services, "sample_comments_for_posts", fake_sample_comments)
    monkeypatch.setattr(services, "analyze_posts_with_ai", fake_analyze)

    game_resp = client.post(
        "/api/games",
        json={"name": "ScopedGame", "subreddit": "scan"},
        headers=owner_headers,
    )
    gid = game_resp.json()["id"]

    run_resp = client.post(f"/api/games/{gid}/scan", headers=owner_headers)
    assert run_resp.status_code == 200

    forged_doc = {
        "_id": str(uuid.uuid4()),
        "game_id": gid,
        "user_id": other_me["user_id"],
        "created_at": datetime.utcnow() + timedelta(minutes=5),
        "posts": [],
        "comments": [],
        "analysis": {"sentiment_label": "Negative", "themes": [], "pain_points": [], "wins": []},
    }
    _run_async(database.db.scan_results.insert_one(forged_doc))

    latest_resp = client.get(f"/api/games/{gid}/latest-result", headers=owner_headers)
    assert latest_resp.status_code == 200
    assert latest_resp.json()["analysis"]["sentiment_label"] == "Positive"
    assert owner_me["user_id"] != other_me["user_id"]
