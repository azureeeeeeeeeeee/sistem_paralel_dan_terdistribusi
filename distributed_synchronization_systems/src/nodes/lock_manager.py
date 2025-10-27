import asyncio
from aiohttp import web
from src.nodes.base_node import BaseNode
from src.utils.config import setup_logger, get_config, get_peers

class DistributedLockManager(BaseNode):
    def __init__(self, port):
        super().__init__(port)
        # Struktur lock:
        # {
        #   "resourceA": {"mode": "exclusive", "owner": "client-5000"},
        #   "resourceB": {"mode": "shared", "owners": {"client-5000", "client-5001"}}
        # }
        self.locks = {}
        self.logger.info(f"Distributed Lock Manager initialized on port {self.port}")

    # ==========================================================
    # ==========  CLIENT HANDLERS  =============================
    # ==========================================================
    async def handle_acquire_lock(self, request):
        if not self.raft.is_leader:
            return web.json_response({"error": "Not a leader"}, status=403)

        data = await request.json()
        resource = data["resource"]
        client_id = data["client_id"]
        mode = data.get("mode", "exclusive")

        lock = self.locks.get(resource)

        # Belum ada lock
        if lock is None:
            self.locks[resource] = {"owner": [client_id], "mode": mode}
            return web.json_response({"status": "lock_acquired"})

        # Shared lock
        if mode == "shared" and lock["mode"] == "shared":
            lock["owner"].append(client_id)
            return web.json_response({"status": "lock_acquired_shared"})

        # Exclusive lock sudah ada
        return web.json_response({"error": "resource_locked"}, status=409)

    async def handle_release_lock(self, request):
        if not self.raft.is_leader:
            return web.json_response({"error": "Not a leader"}, status=403)

        data = await request.json()
        resource = data["resource"]
        client_id = data["client_id"]

        lock = self.locks.get(resource)
        if not lock or client_id not in lock["owner"]:
            return web.json_response({"error": "no_lock_found"}, status=404)

        lock["owner"].remove(client_id)
        if not lock["owner"]:
            del self.locks[resource]

        return web.json_response({"status": "lock_released"})

    async def handle_get_locks(self, request):
        """Debug: lihat semua lock"""
        return web.json_response(self.locks)

    async def handle_status(self, request):
        """Status node dan cluster"""
        status = {
            "state": self.raft.state,
            "leader_id": self.raft.leader_id,
            "port": self.port,
            "locks": len(self.locks),
        }
        return web.json_response(status)

    # ==========================================================
    # ==========  RAFT LOG APPLICATION  ========================
    # ==========================================================
    async def apply_log_entry(self, entry):
        """Dijalankan setelah log di-commit oleh Raft"""
        cmd = entry["command"]
        action = cmd["action"]
        resource = cmd["resource"]
        client = cmd["client_id"]

        if action == "acquire":
            mode = cmd["mode"]
            result = self._acquire(resource, mode, client)
            return {"status": "ok" if result else "failed"}

        elif action == "release":
            result = self._release(resource, client)
            return {"status": "ok" if result else "failed"}

        else:
            self.logger.warning(f"Unknown command: {cmd}")
            return {"status": "error"}

    # ==========================================================
    # ==========  CORE LOCK LOGIC  =============================
    # ==========================================================
    def _acquire(self, resource, mode, client):
        """Internal logic acquire shared/exclusive lock"""
        if resource not in self.locks:
            # Tidak ada lock → langsung bisa diambil
            if mode == "shared":
                self.locks[resource] = {"mode": "shared", "owners": {client}}
                self.logger.info(f"🔓 Shared lock acquired for {resource} by {client}")
            else:
                self.locks[resource] = {"mode": "exclusive", "owner": client}
                self.logger.info(f"🔒 Exclusive lock acquired for {resource} by {client}")
            return True

        # Sudah ada lock
        current = self.locks[resource]

        # CASE 1: Resource di-lock secara shared
        if current["mode"] == "shared":
            if mode == "shared":
                current["owners"].add(client)
                self.logger.info(f"🔁 Shared lock added for {resource} by {client}")
                return True
            else:
                # Minta exclusive tapi masih ada shared lock aktif
                self.logger.warning(f"❌ Cannot acquire EXCLUSIVE lock on {resource}; shared locks active")
                return False

        # CASE 2: Resource di-lock secara exclusive
        elif current["mode"] == "exclusive":
            if current["owner"] == client:
                self.logger.info(f"⚠️ {client} already owns exclusive lock on {resource}")
                return True
            else:
                self.logger.warning(f"❌ Cannot acquire lock; {resource} owned exclusively by {current['owner']}")
                return False

    def _release(self, resource, client):
        """Internal logic release lock"""
        if resource not in self.locks:
            self.logger.warning(f"⚠️ Attempt to release non-existent lock {resource}")
            return False

        current = self.locks[resource]

        if current["mode"] == "shared":
            if client in current["owners"]:
                current["owners"].remove(client)
                self.logger.info(f"🔓 Shared lock released for {resource} by {client}")
                if not current["owners"]:
                    del self.locks[resource]
                    self.logger.info(f"🧹 All shared locks released for {resource}")
                return True
            else:
                self.logger.warning(f"⚠️ {client} has no shared lock on {resource}")
                return False

        elif current["mode"] == "exclusive":
            if current["owner"] == client:
                del self.locks[resource]
                self.logger.info(f"🔓 Exclusive lock released for {resource} by {client}")
                return True
            else:
                self.logger.warning(f"⚠️ {client} tried to release lock owned by {current['owner']}")
                return False

    # ==========================================================
    # ==========  START SERVER  ================================
    # ==========================================================
    async def start(self):
        extra_routes = [
            web.post("/acquire_lock", self.handle_acquire_lock),
            web.post("/release_lock", self.handle_release_lock),
            web.get("/locks", self.handle_get_locks),
            web.get("/status", self.handle_status),
        ]
        await self.start_custom(extra_routes)


if __name__ == "__main__":
    import argparse
    from src.utils.config import get_config

    parser = argparse.ArgumentParser(description="Start a DLM node (Raft-based).")
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    config = get_config()

    if args.port not in range(config['min_port'], config['max_port'] + 1):
        print(f"""
        \n\nPORT NOT IN RANGE {config['min_port']} - {config['max_port']}\n\n
        """)
        exit(1)

    node = DistributedLockManager(args.port)
    print(f"DLM Node started on port {node.port}")
    print(f"Peers: {node.peers}")
    asyncio.run(node.start())
