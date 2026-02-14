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
