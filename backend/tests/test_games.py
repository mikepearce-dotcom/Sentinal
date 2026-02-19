from app import services

import pytest


def test_game_crud(client):
    # sign up and login
    client.post("/api/auth/signup", json={"email": "gamer@example.com", "name": "Gamer", "password": "pass"})
    resp = client.post("/api/auth/login", json={"email": "gamer@example.com", "password": "pass"})
    token = resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    # add game
    data = {"name": "TestGame", "subreddit": "testsub"}
    r = client.post("/api/games", json=data, headers=headers)
    assert r.status_code == 200
    game = r.json()
    assert game["name"] == "TestGame"

    gid = game["id"]
    # get game
    r = client.get(f"/api/games/{gid}", headers=headers)
    assert r.status_code == 200
    # update
    r = client.put(f"/api/games/{gid}", json={"name": "NewName", "subreddit": "testsub"}, headers=headers)
    assert r.status_code == 200
    assert r.json()["name"] == "NewName"
    # delete
    r = client.delete(f"/api/games/{gid}", headers=headers)
    assert r.status_code == 200



def test_discover_subreddits_endpoint(client, monkeypatch):
    client.post(
        "/api/auth/signup",
        json={"email": "finder@example.com", "name": "Finder", "password": "pass"},
    )
    resp = client.post("/api/auth/login", json={"email": "finder@example.com", "password": "pass"})
    token = resp.json()["access_token"]
    headers = {"Authorization": f"Bearer {token}"}

    async def fake_discover(game_name: str, max_results: int = 5):
        assert game_name == "Arc Raiders"
        assert max_results == 3
        return [
            {
                "subreddit": "ArcRaiders",
                "subscribers": 12345,
                "score": 2.1,
                "reason": "High content relevance and consistent discussion",
            }
        ]

    monkeypatch.setattr(services, "discover_subreddits_for_game", fake_discover)

    r = client.get(
        "/api/games/discover-subreddits",
        params={"game_name": "Arc Raiders", "max_results": 3},
        headers=headers,
    )
    assert r.status_code == 200
    payload = r.json()
    assert payload["game_name"] == "Arc Raiders"
    assert len(payload["results"]) == 1
    assert payload["results"][0]["subreddit"] == "ArcRaiders"
