from app import services


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

    # verify results stored
    r = client.get(f"/api/games/{gid}/latest-result", headers=headers)
    assert r.status_code == 200
    assert r.json()["analysis"]["sentiment_label"] == "Positive"
