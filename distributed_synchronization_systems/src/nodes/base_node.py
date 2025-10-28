import asyncio
import random
import time
from aiohttp import web
from src.utils.config import setup_logger, get_config, get_peers
from src.consensus.raft import Raft

class BaseNode:
    def __init__(self, port):
        config = get_config()
        self.port = port
        self.peers = get_peers(port)
        self.logger = setup_logger(port)

        self.raft = Raft(
            port=port,
            peers=self.peers,
            logger=self.logger
        )

    async def handle_root(self, request):
        return web.Response(text=f"Node running on port {self.port}")

    async def handle_heartbeat(self, request):
        data = await request.json()
        response = await self.raft.receive_heartbeat(data)
        return web.json_response(response)

    async def handle_request_vote(self, request):
        data = await request.json()
        response = await self.raft.receive_vote_request(data)
        return web.json_response(response)

    async def handle_append_entries(self, request):
        data = await request.json()
        response = await self.raft.receive_append_entries(data)
        return web.json_response(response)
    
    async def handle_client_command(self, request):
        """Handle client requests — replicate a command through Raft."""
        data = await request.json()

        if self.raft.state != "leader":
            return web.json_response({
                "error": "Not leader",
                "leader_id": self.raft.leader_id
            }, status=400)

        result = await self.raft.replicate_log(data)
        return web.json_response(result)

    async def start_custom(self, extra_routes=None):
        app = web.Application()
        routes = [
            web.get("/", self.handle_root),
            web.post("/heartbeat", self.handle_heartbeat),
            web.post("/request_vote", self.handle_request_vote),
            web.post("/append_entries", self.handle_append_entries),
            web.post("/client_command", self.handle_client_command),
        ]
        if extra_routes:
            routes.extend(extra_routes)
        app.add_routes(routes)

        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", self.port)
        await site.start()

        self.logger.info(f"Node started on port {self.port}, peers: {self.peers}")

        asyncio.create_task(self.raft.run())

        # Biarkan server tetap hidup
        await asyncio.Event().wait()

    async def start(self):
        await self.start_custom()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Start a Raft node.")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    config = get_config()

    if args.port not in range(config['min_port'], config['max_port']+1):
        print(f"""

        \n\n
        PORT NOT IN RANGE {config['min_port']} - {config['max_port']}

        """)
        exit(1)

    node = BaseNode(args.port)
    print(f"Node started on port {node.port}")
    print(f"Peers: {node.peers}")
    # node.start()
    asyncio.run(node.start())
