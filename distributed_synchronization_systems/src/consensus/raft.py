import asyncio
import random
import time
import aiohttp
from urllib.parse import urlparse

class Raft:
    def __init__(self, port, peers, logger):
        """
        port: local port number (int) or identifier
        peers: list of peers. Accepts either "http://localhost:5001" or "localhost:5001" or "127.0.0.1:5001"
        logger: injector logger
        """
        self.port = port
        self.peers = [self._normalize_peer(p) for p in peers]
        self.logger = logger

        # Raft persistent state
        self.state = "follower"
        self.current_term = 0
        self.voted_for = None
        # log is list of wrappers: {"term": int, "index": int, "entry": {...}}
        self.log = []

        # volatile state
        self.commit_index = -1
        self.last_applied = -1

        # timers
        self.timeout = self._new_timeout()
        self.heartbeat = 2
        self.last_heartbeat_time = time.time()

        self.leader_id = None
        self.is_leader = False

        # state machine (injected by node, e.g. DistributedLockManager)
        self.state_machine = None

        # protect log modifications
        self._log_lock = asyncio.Lock()

    # ---------------- helpers ----------------
    def _normalize_peer(self, peer):
        """Ensure peer is an http:// URL (no trailing slash)."""
        if isinstance(peer, int):
            return f"http://localhost:{peer}"
        if peer.startswith("http://") or peer.startswith("https://"):
            url = peer
        else:
            url = f"http://{peer}"
        return url.rstrip("/")

    def _new_log_index(self):
        return len(self.log)

    def _new_timeout(self):
        return random.uniform(5, 9)

    # ---------------- main run loop ----------------
    async def run(self):
        """
        Main loop. This will run forever; the node should run this via asyncio.create_task or asyncio.run.
        """
        while True:
            try:
                if self.state == "follower":
                    await self._follower_loop()
                elif self.state == "candidate":
                    await self._candidate_loop()
                elif self.state == "leader":
                    await self._leader_loop()
            except Exception as e:
                self.logger.exception(f"[{self.port}] Unexpected error in run loop: {e}")
            await asyncio.sleep(0.01)

    # ---------- follower / candidate / leader loops ----------
    async def _follower_loop(self):
        self.logger.info(f"[{self.port}] State = FOLLOWER (term={self.current_term})")
        self.is_leader = False
        start = time.time()
        while self.state == "follower":
            await asyncio.sleep(0.1)
            if time.time() - self.last_heartbeat_time > self.timeout:
                self.logger.info(f"[{self.port}] Election timeout reached -> switching to CANDIDATE")
                self.state = "candidate"
                return

    async def _candidate_loop(self):
        # start election
        self.current_term += 1
        self.voted_for = self.port
        votes = 1
        total_nodes = len(self.peers) + 1
        majority = (total_nodes // 2) + 1
        self.timeout = self._new_timeout()
        self.last_heartbeat_time = time.time()

        self.logger.info(f"[{self.port}] Starting election for term {self.current_term} (need {majority} votes)")

        # ask votes in parallel
        tasks = [self._request_vote(peer) for peer in self.peers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, dict) and res.get("vote_granted"):
                votes += 1

        if votes >= majority:
            self.logger.info(f"[{self.port}] Won election ({votes}/{total_nodes}) -> LEADER")
            self.state = "leader"
            self.leader_id = self.port
            self.is_leader = True
            # initialize leader volatile state if needed (not storing nextIndex/matchIndex in prototype)
        else:
            self.logger.info(f"[{self.port}] Lost election ({votes}/{total_nodes}) -> FOLLOWER")
            self.state = "follower"
            self.is_leader = False

    async def _leader_loop(self):
        self.logger.info(f"[{self.port}] Acting as LEADER for term {self.current_term}")
        self.is_leader = True
        # send initial heartbeat immediately
        while self.state == "leader":
            await self._send_heartbeats()
            await asyncio.sleep(self.heartbeat)

    # ------------- public API for leader to replicate entry -------------
    async def replicate_log(self, entry):
        """
        Leader appends entry to its log and tries to replicate it to followers.
        Returns dict with success flag, index, and ack count.
        """
        if not self.is_leader:
            return {"error": "not_leader", "leader_id": self.leader_id}

        async with self._log_lock:
            index = self._new_log_index()
            wrapper = {"term": self.current_term, "index": index, "entry": entry}
            self.log.append(wrapper)
            self.logger.info(f"[{self.port}] Appended local log idx={index} term={self.current_term} entry={entry}")

        # Broadcast append_entries with this single entry (include prev_log info)
        prev_index = index - 1
        prev_term = self.log[prev_index]["term"] if prev_index >= 0 else 0

        tasks = [self._send_append_entries(peer,
                                           entries=[wrapper],
                                           prev_log_index=prev_index,
                                           prev_log_term=prev_term) for peer in self.peers]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        success_acks = 1  # leader itself
        for res in results:
            if isinstance(res, dict) and res.get("success"):
                success_acks += 1

        total_nodes = len(self.peers) + 1
        majority = (total_nodes // 2) + 1

        if success_acks >= majority:
            # commit and apply
            async with self._log_lock:
                self.commit_index = wrapper["index"]
            await self._apply_committed_entries()
            self.logger.info(f"[{self.port}] Entry committed idx={wrapper['index']} (acks={success_acks}/{total_nodes})")
            return {"success": True, "index": wrapper["index"], "acks": success_acks}
        else:
            self.logger.warning(f"[{self.port}] Not enough acks to commit ({success_acks}/{total_nodes})")
            return {"success": False, "acks": success_acks, "required": majority}

    # ------------- internal: apply committed entries to state machine -------------
    async def _apply_committed_entries(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            wrapper = self.log[self.last_applied]
            await self._apply_log_entry(wrapper)

    async def _apply_log_entry(self, wrapper):
        entry = wrapper.get("entry")
        if not entry:
            return
        if self.state_machine and hasattr(self.state_machine, "apply"):
            try:
                await self.state_machine.apply(entry)
                self.logger.info(f"[{self.port}] Applied log idx={wrapper.get('index')} entry={entry}")
            except Exception as e:
                self.logger.exception(f"[{self.port}] Error applying log entry: {e}")
        else:
            self.logger.debug(f"[{self.port}] No state_machine to apply entry: {entry}")

    # ------------- networking helpers -------------
    async def _send_heartbeats(self):
        prev_index = len(self.log) - 1
        prev_term = self.log[prev_index]["term"] if prev_index >= 0 else 0
        tasks = [
            self._send_append_entries(peer, entries=[], prev_log_index=prev_index, prev_log_term=prev_term)
            for peer in self.peers
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for peer, res in zip(self.peers, results):
            if isinstance(res, dict) and res.get("success"):
                self.logger.info(f"[{self.port}] Heartbeat ACK from {peer}")
            else:
                self.logger.warning(f"[{self.port}] Heartbeat FAILED to {peer} -> {res}")


    async def _send_append_entries(self, peer, entries, prev_log_index, prev_log_term):
        url = f"{peer}/append_entries"
        self.logger.info(f"Sending AppendEntries to {url}")
        payload = {
            "leader_id": self.port,
            "term": self.current_term,
            "prev_log_index": prev_log_index,
            "prev_log_term": prev_log_term,
            "entries": entries,
            "leader_commit": self.commit_index
        }
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, json=payload, timeout=5) as resp:
                    data = await resp.json()
                    return data
            except Exception as e:
                self.logger.debug(f"[{self.port}] _send_append_entries to {peer} failed: {e}")
                return {"success": False}

    async def _request_vote(self, peer):
        url = f"{peer}/request_vote"
        payload = {
            "candidate_id": self.port,
            "term": self.current_term,
            # optionally include lastLogIndex/lastLogTerm for real Raft (not implemented here)
        }
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(url, json=payload, timeout=5) as resp:
                    data = await resp.json()
                    return data
            except Exception as e:
                self.logger.debug(f"[{self.port}] _request_vote to {peer} failed: {e}")
                return {"vote_granted": False}

    # ------------- RPC handlers called by HTTP endpoints in BaseNode -------------
    async def receive_heartbeat(self, data):
        """
        Simpler heartbeat handler (keeps last_heartbeat and term).
        """
        leader_term = data.get("term", 0)
        leader_id = data.get("leader_id")
        if leader_term >= self.current_term:
            self.current_term = leader_term
            self.state = "follower"
            self.leader_id = leader_id
            self.last_heartbeat_time = time.time()
            self.logger.debug(f"[{self.port}] Heartbeat received from {leader_id} (term {leader_term})")
            return {"success": True, "term": self.current_term}
        else:
            return {"success": False, "term": self.current_term}

    async def receive_vote_request(self, data):
        """
        Vote granting: naive implementation:
        - grant if candidate_term > current_term (or equal and haven't voted)
        """
        candidate_term = data.get("term", 0)
        candidate_id = data.get("candidate_id")

        # If candidate's term is newer, update term and reset voted_for
        if candidate_term > self.current_term:
            self.current_term = candidate_term
            self.voted_for = None
            self.state = "follower"

        grant = False
        if candidate_term == self.current_term:
            if self.voted_for is None or self.voted_for == candidate_id:
                self.voted_for = candidate_id
                grant = True
                self.last_heartbeat_time = time.time()
                self.logger.debug(f"[{self.port}] Voted for {candidate_id} in term {candidate_term}")

        return {"term": self.current_term, "vote_granted": grant}

    async def receive_append_entries(self, data):
        """
        Full AppendEntries handler with prev_log consistency check and conflict resolution.
        Expected payload keys: term, leader_id, prev_log_index, prev_log_term, entries, leader_commit
        """
        leader_term = data.get("term", 0)
        leader_id = data.get("leader_id")
        prev_log_index = data.get("prev_log_index", -1)
        prev_log_term = data.get("prev_log_term", 0)
        entries = data.get("entries", [])
        leader_commit = data.get("leader_commit", -1)

        # 1) Reply false if term < currentTerm
        if leader_term < self.current_term:
            return {"success": False, "term": self.current_term}

        # 2) Update state from leader
        self.leader_id = leader_id
        self.current_term = leader_term
        self.state = "follower"
        self.last_heartbeat_time = time.time()

        if not data.get("entries"):
            self.logger.info(f"[{self.port}] Heartbeat RECEIVED from leader {leader_id} (term {leader_term})")


        async with self._log_lock:
            # 3) Check if log contains an entry at prev_log_index whose term matches prev_log_term
            if prev_log_index >= 0:
                if prev_log_index >= len(self.log):
                    # follower log is not as long as leader expects -> reject (leader should decrement prev_log_index in advanced impl)
                    self.logger.debug(f"[{self.port}] Missing prev_log_index {prev_log_index} (local_len={len(self.log)}) -> reject")
                    return {"success": False, "term": self.current_term}
                local_prev_term = self.log[prev_log_index]["term"]
                if local_prev_term != prev_log_term:
                    # conflict: remove the entry and all that follow it
                    self.logger.debug(f"[{self.port}] Log term conflict at idx {prev_log_index} (local_term={local_prev_term} != prev_term={prev_log_term}) -> truncating")
                    # truncate up to prev_log_index (keep entries before conflict)
                    self.log = self.log[:prev_log_index]
                    return {"success": False, "term": self.current_term}

            # 4) Append any new entries not already in the log
            for incoming in entries:
                idx = incoming.get("index")
                # Defensive: if incoming has index field, ensure consistent placement
                if idx is None:
                    # if no index provided, append at end with recalculated index
                    incoming_idx = len(self.log)
                    incoming["index"] = incoming_idx
                    self.log.append(incoming)
                    self.logger.debug(f"[{self.port}] Appended (no idx) entry -> idx {incoming_idx}")
                else:
                    if idx < len(self.log):
                        # if existing entry at idx has different term -> replace/truncate
                        if self.log[idx]["term"] != incoming["term"]:
                            self.logger.debug(f"[{self.port}] Conflict at idx {idx}: replacing and truncating following entries")
                            self.log = self.log[:idx]
                            self.log.append(incoming)
                        else:
                            # same term -> keep (idempotent)
                            self.logger.debug(f"[{self.port}] Entry at idx {idx} already present")
                    elif idx == len(self.log):
                        # append in-order
                        self.log.append(incoming)
                        self.logger.debug(f"[{self.port}] Appended entry idx={idx}")
                    else:
                        # gap: pad/trust incoming and append (simple approach)
                        self.logger.debug(f"[{self.port}] Gap at idx {idx} (local_len={len(self.log)}). Padding and appending.")
                        # pad with None wrappers until idx
                        while len(self.log) < idx:
                            self.log.append({"term": 0, "index": len(self.log), "entry": None})
                        self.log.append(incoming)

            # 5) Update commit index
            if leader_commit is not None and leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log) - 1)

        # 6) Apply committed but not applied entries
        await self._apply_committed_entries()

        return {"success": True, "term": self.current_term}

    