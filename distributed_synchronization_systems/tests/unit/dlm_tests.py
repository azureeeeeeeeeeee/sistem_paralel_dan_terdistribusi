import pytest
import aiohttp

BASE_URL = "http://127.0.0.1:8000"

@pytest.mark.asyncio
async def test_1_exclusive_lock_acquire_and_release():
    """ClientA acquire & release exclusive lock successfully"""
    async with aiohttp.ClientSession() as session:
        # Acquire
        res = await session.post(f"{BASE_URL}/acquire_lock", json={
            "resource": "res1",
            "mode": "exclusive",
            "client_id": "clientA"
        })
        assert res.status == 200
        data = await res.json()
        assert "error" not in data

        # Release
        res = await session.post(f"{BASE_URL}/release_lock", json={
            "resource": "res1",
            "client_id": "clientA"
        })
        assert res.status == 200
        data = await res.json()
        assert "error" not in data


@pytest.mark.asyncio
async def test_2_exclusive_blocked_if_already_locked():
    """ClientB cannot acquire exclusive lock if already held by ClientA"""
    async with aiohttp.ClientSession() as session:
        # ClientA lock first
        await session.post(f"{BASE_URL}/acquire_lock", json={
            "resource": "res2",
            "mode": "exclusive",
            "client_id": "clientA"
        })

        # ClientB try same resource
        res = await session.post(f"{BASE_URL}/acquire_lock", json={
            "resource": "res2",
            "mode": "exclusive",
            "client_id": "clientB"
        })
        data = await res.json()
        assert res.status == 400 or "error" in data

        # Cleanup
        await session.post(f"{BASE_URL}/release_lock", json={
            "resource": "res2",
            "client_id": "clientA"
        })


@pytest.mark.asyncio
async def test_3_shared_lock_multiple_clients():
    """Multiple clients can acquire shared lock"""
    async with aiohttp.ClientSession() as session:
        # clientA acquire shared
        res = await session.post(f"{BASE_URL}/acquire_lock", json={
            "resource": "res3",
            "mode": "shared",
            "client_id": "clientA"
        })
        assert res.status == 200

        # clientB also acquire shared
        res = await session.post(f"{BASE_URL}/acquire_lock", json={
            "resource": "res3",
            "mode": "shared",
            "client_id": "clientB"
        })
        assert res.status == 200

        # Cleanup
        await session.post(f"{BASE_URL}/release_lock", json={
            "resource": "res3",
            "client_id": "clientA"
        })
        await session.post(f"{BASE_URL}/release_lock", json={
            "resource": "res3",
            "client_id": "clientB"
        })


@pytest.mark.asyncio
async def test_4_shared_block_exclusive():
    """Exclusive lock request should fail if shared locks exist"""
    async with aiohttp.ClientSession() as session:
        # Two shared locks exist
        await session.post(f"{BASE_URL}/acquire_lock", json={
            "resource": "res4",
            "mode": "shared",
            "client_id": "clientA"
        })
        await session.post(f"{BASE_URL}/acquire_lock", json={
            "resource": "res4",
            "mode": "shared",
            "client_id": "clientB"
        })

        # Try exclusive
        res = await session.post(f"{BASE_URL}/acquire_lock", json={
            "resource": "res4",
            "mode": "exclusive",
            "client_id": "clientC"
        })
        data = await res.json()
        assert res.status == 400 or "error" in data

        # Cleanup
        await session.post(f"{BASE_URL}/release_lock", json={
            "resource": "res4",
            "client_id": "clientA"
        })
        await session.post(f"{BASE_URL}/release_lock", json={
            "resource": "res4",
            "client_id": "clientB"
        })


@pytest.mark.asyncio
async def test_5_release_by_non_owner_fails():
    """Releasing lock by wrong client should fail"""
    async with aiohttp.ClientSession() as session:
        # ClientA locks resource
        await session.post(f"{BASE_URL}/acquire_lock", json={
            "resource": "res5",
            "mode": "exclusive",
            "client_id": "clientA"
        })

        # ClientB tries to release (should fail)
        res = await session.post(f"{BASE_URL}/release_lock", json={
            "resource": "res5",
            "client_id": "clientB"
        })
        data = await res.json()
        assert res.status == 400 or "error" in data

        # Cleanup (correct release)
        await session.post(f"{BASE_URL}/release_lock", json={
            "resource": "res5",
            "client_id": "clientA"
        })