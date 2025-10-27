import pytest
import aiohttp
import asyncio

LEADER_PORT = 8000
FOLLOWER_PORT = 8001
BASE_LEADER = f"http://localhost:{LEADER_PORT}"
BASE_FOLLOWER = f"http://localhost:{FOLLOWER_PORT}"

@pytest.mark.asyncio
async def test_leader_set_key():
    async with aiohttp.ClientSession() as session:
        res = await session.post(f"{BASE_LEADER}/set", json={"key": "item1", "value": "value1"})
        data = await res.json()
        assert res.status == 200
        assert data["key"] == "item1"
        assert data["value"] == "value1"

@pytest.mark.asyncio
async def test_follower_set_key_redirect():
    async with aiohttp.ClientSession() as session:
        res = await session.post(f"{BASE_FOLLOWER}/set", json={"key": "item1", "value": "value1"})
        data = await res.json()
        assert res.status == 400
        assert "Not leader" in data["error"]
        assert data["leader_id"] == LEADER_PORT

@pytest.mark.asyncio
async def test_leader_get_key_hit():
    async with aiohttp.ClientSession() as session:
        res = await session.post(f"{BASE_LEADER}/get", json={"key": "item1"})
        data = await res.json()
        assert res.status == 200
        assert data["key"] == "item1"
        assert data["value"] == "value1"
        assert data["state"] in ["M", "E", "S", "I"]
        assert "metrics" in data

@pytest.mark.asyncio
async def test_follower_get_key_fetch_from_leader():
    async with aiohttp.ClientSession() as session:
        # pastikan follower cache kosong
        res = await session.post(f"{BASE_FOLLOWER}/get", json={"key": "item1"})
        data = await res.json()
        assert res.status == 200
        assert data["key"] == "item1"
        assert data["value"] == "value1"
        assert data["state"] in ["S", "E", "M", "I"]
        assert "metrics" in data

@pytest.mark.asyncio
async def test_leader_invalidate_key():
    async with aiohttp.ClientSession() as session:
        res = await session.post(f"{BASE_LEADER}/invalidate", json={"key": "item1"})
        data = await res.json()
        assert res.status == 200
        assert data["status"] == "invalidated"
        assert data["key"] == "item1"