import asyncio
from aiohttp import web, ClientSession
from src.nodes.base_node import BaseNode
from src.utils.config import setup_logger, get_config, get_peers
import hashlib
import redis.asyncio as aioredis
import os

class DistributedQueueNode(BaseNode):
    def __init__(self, port):
        super().__init__(port)
        self.queues = {}  # memory queue
        self.delivered_messages = {}
        self.redis = None
        self.session = None
        self.self_url = f"http://localhost:{self.port}"  # URL node ini

    # ----------------------
    # REDIS INIT (Docker-ready)
    # ----------------------
    async def init_redis(self):
        REDIS_HOST = os.getenv("REDIS_HOST", "redis")  # default ke service name docker
        REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
        for attempt in range(5):
            try:
                self.redis = await aioredis.from_url(f"redis://{REDIS_HOST}:{REDIS_PORT}")
                self.logger.info(f"✅ Connected to Redis at {REDIS_HOST}:{REDIS_PORT}")
                return
            except Exception as e:
                self.logger.warning(f"Redis not ready (attempt {attempt+1}/5): {e}")
                await asyncio.sleep(2)
        raise ConnectionError(f"Could not connect to Redis at {REDIS_HOST}:{REDIS_PORT}")

    # ----------------------
    # CONSISTENT HASHING
    # ----------------------
    def get_node_for_key(self, key):
        nodes = sorted(self.peers + [self.self_url])
        h = int(hashlib.sha256(key.encode()).hexdigest(), 16)
        return nodes[h % len(nodes)]

    # ----------------------
    # ENQUEUE
    # ----------------------
    async def handle_enqueue(self, request):
        data = await request.json()
        queue = data["queue"]
        message = data["message"]
        client_id = data.get("client_id", f"client-{self.port}")
        forwarded = data.get("forwarded", False)

        target_node = self.get_node_for_key(queue)

        # Forward jika peer
        if target_node != self.self_url and not forwarded:
            data["forwarded"] = True
            async with ClientSession() as session:
                async with session.post(f"{target_node}/enqueue", json=data) as resp:
                    result = await resp.json()
                    return web.json_response(result, status=resp.status)

        # Jika node owner tapi bukan leader
        if self.raft.state != "leader":
            return web.json_response(
                {"error": "Not leader", "leader_id": self.raft.leader_id},
                status=400,
            )

        # Leader → replicate log & apply
        command = {"action": "enqueue", "queue": queue, "message": message, "client_id": client_id}
        result = await self.raft.replicate_log(command)
        await self.apply_log_entry({"command": command})

        self.logger.info(f"Queue after enqueue: {self.queues}")

        return web.json_response({
            "success": True,
            "index": result.get("index"),
            "acks": result.get("acks"),
            "message": f"Message enqueued to {queue} via leader"
        })

    # ----------------------
    # DEQUEUE
    # ----------------------
    async def handle_dequeue(self, request):
        data = await request.json()
        queue = data["queue"]
        client_id = data.get("client_id", f"client-{self.port}")
        target_node = self.get_node_for_key(queue)

        if target_node != self.self_url:
            async with ClientSession() as session:
                async with session.post(f"{target_node}/dequeue", json=data) as resp:
                    return web.json_response(await resp.json(), status=resp.status)

        self.logger.info(f"Dequeue request for queue '{queue}', current queue: {self.queues.get(queue, [])}")
        if queue not in self.queues or not self.queues[queue]:
            return web.json_response({"message": None})

        message = self.queues[queue].pop(0)
        self.delivered_messages.setdefault(queue, {})[message] = client_id

        await self.redis.lpop(queue)

        return web.json_response({"message": message})

    # ----------------------
    # ACK
    # ----------------------
    async def handle_ack(self, request):
        data = await request.json()
        queue = data["queue"]
        message = data["message"]
        client_id = data.get("client_id", f"client-{self.port}")

        if queue in self.delivered_messages and message in self.delivered_messages[queue]:
            if self.delivered_messages[queue][message] == client_id:
                del self.delivered_messages[queue][message]

        return web.json_response({"status": "acknowledged"})

    # ----------------------
    # APPLY LOG ENTRY
    # ----------------------
    async def apply_log_entry(self, entry):
        cmd = entry["command"]
        if cmd["action"] == "enqueue":
            queue = cmd["queue"]
            message = cmd["message"]
            if queue not in self.queues:
                self.queues[queue] = []
            self.queues[queue].append(message)
            await self.redis.rpush(queue, message)
            self.logger.info(f"📥 Message enqueued to {queue}: {message}")
        else:
            self.logger.warning(f"Unknown command: {cmd}")

    # ----------------------
    # RECOVERY
    # ----------------------
    async def recover_from_redis(self):
        keys = await self.redis.keys("*")
        for queue in keys:
            messages = await self.redis.lrange(queue, 0, -1)
            self.queues[queue] = messages
        self.logger.info(f"♻️ Recovered queues from Redis: {list(self.queues.keys())}")

    async def recover_from_raft(self):
        if hasattr(self.raft, "log"):
            for entry in self.raft.log:
                await self.apply_log_entry(entry)
            self.logger.info("♻️ Recovered state from Raft logs")

    # ----------------------
    # START NODE
    # ----------------------
    async def start(self):
        await self.init_redis()
        await self.recover_from_redis()
        await self.recover_from_raft()

        extra_routes = [
            web.post("/enqueue", self.handle_enqueue),
            web.post("/dequeue", self.handle_dequeue),
            web.post("/ack", self.handle_ack),
        ]
        self.logger.info("Registering extra routes for /enqueue, /dequeue, /ack")
        await self.start_custom(extra_routes)


# ----------------------
# RUN AS SCRIPT
# ----------------------
if __name__ == "__main__":
    import argparse
    from src.utils.config import get_config

    parser = argparse.ArgumentParser(description="Start a Distributed Queue node (Raft-based).")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    config = get_config()
    if args.port not in range(config["min_port"], config["max_port"] + 1):
        print(f"\n\nPORT NOT IN RANGE {config['min_port']} - {config['max_port']}\n\n")
        exit(1)

    node = DistributedQueueNode(args.port)
    print(f"Queue Node started on port {node.port}")
    print(f"Peers: {node.peers}")
    asyncio.run(node.start())
