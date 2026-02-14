import pytest
from app import services


def test_scan_endpoint(client, monkeypatch):
    # create user and login
    client.post("/api/auth/signup", json={"email": "scan@example.com", "name": "Scanner", "password": "pass"})
    resp = client.post("/api/auth/login", json={"email": "scan@example.com", "password": "pass"})
    token = resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # patch external service calls to return predictable data
    monkeypatch.setattr(services, "fetch_reddit_posts", lambda subreddit, limit=100: [{"id": "1", "title": "post1"}])
    monkeypatch.setattr(services, "fetch_comments_for_post", lambda pid, limit=20: [{"body": "comment"}])
    monkeypatch.setattr(services, "analyze_posts_with_ai", lambda posts, comments: {"sentiment_label": "positive"})

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
    assert r.json()["analysis"]["sentiment_label"] == "positive"
