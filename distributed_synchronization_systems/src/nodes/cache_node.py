import asyncio
from aiohttp import web
from src.nodes.base_node import BaseNode
from src.utils.config import setup_logger, get_config, get_peers
import aiohttp
from collections import OrderedDict

MAX_CACHE_SIZE = 50

class CacheNode(BaseNode):
    def __init__(self, port):
        super().__init__(port)
        self.cache = OrderedDict()  # LRU
        self.session = None
        self.metrics = {"hits": 0, "misses": 0, "evictions": 0}
        self.logger.info(f"Cache Node initialized on port {self.port}")

    async def handle_get(self, request):
        data = await request.json()
        key = data["key"]

        cache_entry = self.cache.get(key, {"value": None, "state": "I"})
        value, state = cache_entry["value"], cache_entry["state"]

        if state == "I" or value is None:
            self.metrics["misses"] += 1
            self.logger.info(f"Cache MISS for key '{key}' (state={state})")

            if self.raft.is_leader:
                value = self.cache.get(key, {}).get("value")
                if value is None:
                    self.cache[key] = {"value": None, "state": "I"}
                else:
                    self.cache[key]["state"] = "M"
            elif self.raft.leader_id:
                leader_peer = next((p for p in self.peers if str(self.raft.leader_id) in p), None)
                if leader_peer:
                    leader_url = f"{leader_peer}/get"
                    try:
                        async with aiohttp.ClientSession() as session:
                            async with session.post(leader_url, json={"key": key}, timeout=5) as resp:
                                if resp.status == 200:
                                    leader_data = await resp.json()
                                    value = leader_data.get("value")
                                    self.cache[key] = {
                                        "value": value,
                                        "state": "S" if value is not None else "I"
                                    }
                                    self.logger.info(f"Follower fetched key '{key}' from leader successfully")
                                else:
                                    value = None
                                    self.cache[key] = {"value": None, "state": "I"}
                                    self.logger.warning(f"Leader returned status {resp.status} for key {key}")
                    except Exception as e:
                        self.logger.warning(f"Failed to fetch key '{key}' from leader: {e}")
                        value = None
                        self.cache[key] = {"value": None, "state": "I"}
                else:
                    self.logger.warning(f"Could not find leader URL for leader_id={self.raft.leader_id}")
                    value = None
                    self.cache[key] = {"value": None, "state": "I"}
            else:
                value = None
                self.cache[key] = {"value": None, "state": "I"}
        else:
            self.metrics["hits"] += 1
            self.logger.info(f"Cache HIT for key '{key}', state={state}")

        if key in self.cache:
            self.cache.move_to_end(key)

        return web.json_response({
            "key": key,
            "value": value,
            "state": self.cache[key]["state"],
            "metrics": self.metrics
        })

    async def handle_set(self, request):
        data = await request.json()
        key = data["key"]
        value = data["value"]

        if not self.raft.is_leader:
            return web.json_response({
                "error": "Not leader",
                "leader_id": self.raft.leader_id
            }, status=400)

        self.cache[key] = {"value": value, "state": "M"}
        self.cache.move_to_end(key)
        self.logger.info(f"SET key '{key}' -> '{value}' (state=M)")

        if len(self.cache) > MAX_CACHE_SIZE:
            oldest_key, _ = self.cache.popitem(last=False)
            self.metrics["evictions"] += 1
            self.logger.info(f"LRU evict key '{oldest_key}'")

        await self.broadcast_invalidate(key, source="local")

        return web.json_response({"status": "ok", "key": key, "value": value})

    async def handle_invalidate(self, request):
        data = await request.json()
        key = data["key"]
        source = data.get("source", "peer")

        if key in self.cache:
            self.cache[key]["state"] = "I"
            self.cache[key]["value"] = None
            self.logger.info(f"INVALIDATE key '{key}' -> state=I (value removed)")

            if source == "local" and self.raft.is_leader:
                await self.broadcast_invalidate(key, source="local")

        return web.json_response({"status": "invalidated", "key": key})

    async def broadcast_invalidate(self, key, source="peer"):
        if not self.session:
            self.session = aiohttp.ClientSession()

        tasks = []
        for peer in self.peers:
            url = f"{peer}/invalidate"
            tasks.append(self._send_invalidate(url, key, source))
        await asyncio.gather(*tasks)
        self.logger.info(f"Broadcasted invalidate for key '{key}' to peers: {self.peers}")

    async def _send_invalidate(self, url, key, source="peer"):
        try:
            async with self.session.post(url, json={"key": key, "source": source}, timeout=5) as resp:
                await resp.text()
        except Exception as e:
            self.logger.warning(f"Failed to send invalidate to {url}: {e}")

    async def start(self):
        self.session = aiohttp.ClientSession()
        extra_routes = [
            web.post("/get", self.handle_get),
            web.post("/set", self.handle_set),
            web.post("/invalidate", self.handle_invalidate),
        ]
        await self.start_custom(extra_routes)

        async def cleanup(app):
            if self.session:
                await self.session.close()
        app = web.get_app()
        app.on_cleanup.append(cleanup)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Start a Distributed Cache node (MESI full).")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    config = get_config()
    if args.port not in range(config['min_port'], config['max_port'] + 1):
        print(f"\nPORT NOT IN RANGE {config['min_port']} - {config['max_port']}\n")
        exit(1)

    node = CacheNode(args.port)
    print(f"Cache Node started on port {node.port}")
    print(f"Peers: {node.peers}")
    asyncio.run(node.start())
