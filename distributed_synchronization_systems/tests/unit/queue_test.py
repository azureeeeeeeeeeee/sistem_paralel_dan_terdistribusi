# tests/unit/queue_live_test.py
import pytest
import asyncio
from aiohttp import ClientSession

LEADER_PORT = 9002
FOLLOWER_PORT = 9001

@pytest.mark.asyncio
async def test_enqueue_leader():
    async with ClientSession() as session:
        resp = await session.post(f"http://localhost:{LEADER_PORT}/enqueue",
                                  json={"queue": "test-new-enqueue", "message": "msg1"})
        data = await resp.json()
        assert resp.status == 200
        assert data.get("success") is True

@pytest.mark.asyncio
async def test_enqueue_follower_forward():
    async with ClientSession() as session:
        resp = await session.post(f"http://localhost:{FOLLOWER_PORT}/enqueue",
                                  json={"queue": "test-new-enqueue", "message": "msg2"})
        data = await resp.json()
        assert resp.status == 200
        assert data.get("success") is True

@pytest.mark.asyncio
async def test_dequeue_leader():
    async with ClientSession() as session:
        resp = await session.post(f"http://localhost:{LEADER_PORT}/dequeue",
                                  json={"queue": "test-new-enqueue"})
        data = await resp.json()
        assert resp.status == 200
        assert "message" in data
        assert data["message"] in ["msg1", "msg2"]

@pytest.mark.asyncio
async def test_ack_message():
    async with ClientSession() as session:
        # Ambil pesan baru dulu
        resp = await session.post(f"http://localhost:{LEADER_PORT}/dequeue",
                                  json={"queue": "test-new-enqueue"})
        msg_data = await resp.json()
        message = msg_data.get("message")
        if message is None:
            pytest.skip("Queue kosong, skip ack test")

        # Ack pesan
        ack_resp = await session.post(f"http://localhost:{LEADER_PORT}/ack",
                                      json={"queue": "test-new-enqueue", "message": message})
        ack_data = await ack_resp.json()
        assert ack_resp.status == 200
        assert ack_data.get("status") == "acknowledged"

@pytest.mark.asyncio
async def test_dequeue_empty_queue():
    async with ClientSession() as session:
        # Pastikan queue kosong
        resp = await session.post(f"http://localhost:{LEADER_PORT}/dequeue",
                                  json={"queue": "empty-queue"})
        data = await resp.json()
        assert resp.status == 200
        assert data.get("message") is None
