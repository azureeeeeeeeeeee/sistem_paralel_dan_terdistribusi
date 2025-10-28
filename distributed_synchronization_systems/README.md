
# Distributed Synchronization System

---

# 1. Cluster Lock Manager

## 1.1 Arsitektur Sistem
Cluster Lock Manager menggunakan **Raft Consensus Algorithm** untuk menjaga konsistensi lock di sistem terdistribusi. Terdapat minimal 3 node yang saling berkomunikasi. Setiap node dapat menjadi leader atau follower.

- **Leader** bertanggung jawab memproses permintaan **exclusive lock**.
- **Shared lock** bisa diproses oleh beberapa node tergantung konsensus.
- Lock state tersimpan di Redis untuk recovery.

## 1.2 Algoritma yang Digunakan
**Raft Consensus untuk Distributed Lock:**

1. **Leader Election:** Node memilih leader saat startup atau leader gagal.
2. **Log Replication:** Permintaan lock ditulis ke log leader dan direplikasi ke follower.
3. **Commit:** Setelah mayoritas follower mengkonfirmasi, lock dianggap granted.
4. **Deadlock Detection:** Monitor lock yang menunggu > threshold, batalkan circular lock.

**Jenis Lock:**  
- Shared (read)  
- Exclusive (write)


## 1.3 API Documentation

### Acquire Lock (POST /acquire_lock)

```text
POST /acquire_lock
{
  "resource": "file123",
  "client_id": "client-5000",
  "mode": "exclusive"  # optional, default exclusive
}

Response 200:
{
  "status": "lock_acquired"
}

Response 200 (shared lock added):
{
  "status": "lock_acquired_shared"
}

Response 403 (not leader):
{
  "error": "Not a leader"
}

Response 409 (lock conflict):
{
  "error": "resource_locked"
}
```

### Release Lock (POST /release_lock)

```text
POST /release_lock
{
  "resource": "file123",
  "client_id": "client-5000"
}

Response 200:
{
  "status": "lock_released"
}

Response 403 (not leader):
{
  "error": "Not a leader"
}

Response 404 (no lock found):
{
  "error": "no_lock_found"
}
```

## 1.4 Deployment Guide

**Deployment:**

```bash
docker-compose -f docker/docker-compose.lock_manager.yml up --build
```

---

# 2. Cluster Cache Node

## 2.1 Arsitektur Sistem
Cache Node menggunakan **MESI protocol** untuk menjaga cache coherence antar node.  



- **LRU cache replacement policy** diterapkan di setiap node.
- Update propagation dikontrol MESI → invalidasi node lain saat update.

## 2.2 Algoritma yang Digunakan
1. **MESI Protocol:**
   - **Modified**: hanya di node ini
   - **Exclusive**: node punya copy eksklusif
   - **Shared**: node lain bisa baca
   - **Invalid**: cache perlu di-refresh
2. **LRU Replacement:**
   - Saat cache penuh, entry yang **paling lama tidak diakses** akan dihapus

## 2.3 API Documentation

### Get Cache (POST /get)

```text
POST /get
{
  "key": "file123"
}

Response 200 (cache hit):
{
  "key": "file123",
  "value": "Hello World",
  "state": "M/S",
  "metrics": {"hits": 1, "misses": 0, "evictions": 0}
}

Response 200 (cache miss):
{
  "key": "file123",
  "value": null,
  "state": "I",
  "metrics": {"hits": 0, "misses": 1, "evictions": 0}
}
```

### Set Cache (POST /set)
```
POST /set
{
  "key": "file123",
  "value": "Hello World"
}

Response 200:
{
  "status": "ok",
  "key": "file123",
  "value": "Hello World"
}

Response 400 (not leader):
{
  "error": "Not leader",
  "leader_id": "node-7000"
}
```

### Invalidate (POST /invalidate)
```
POST /invalidate
{
  "key": "file123",
  "source": "peer"  # optional, default peer
}

Response 200:
{
  "status": "invalidated",
  "key": "file123"
}
```


## 2.4 Deployment Guide

**Deployment:**

```bash
docker-compose -f docker/docker-compose.cache.yml up --build
```

---

# 3. Cluster Queue Node

## 3.1 Arsitektur Sistem
Queue Node menggunakan **distributed queue** berbasis **consistent hashing**.  

- Semua node bisa **dequeue**
- Hanya **leader** yang bisa **enqueue**
- Message persistence disimpan di Redis

## 3.2 Algoritma yang Digunakan
1. **Consistent Hashing** → tentukan node yang menyimpan queue tertentu  
2. **Leader-based enqueue** → menjaga order konsisten  
3. **Message Persistence** → Redis menyimpan state untuk recovery  

## 3.3 API Documentation
### Enqueue Message (POST /enqueue)

```text
POST /enqueue
{
  "queue": "tasks",
  "message": "Process data",
  "client_id": "client-5000"  # optional
}

Response 200:
{
  "success": true,
  "index": 5,
  "acks": ["node-7000", "node-7001"],
  "message": "Message enqueued to tasks via leader"
}

Response 400 (not leader):
{
  "error": "Not leader",
  "leader_id": "node-7000"
}
```

### Dequeue Message (POST /dequeue)
```
POST /dequeue
{
  "queue": "tasks",
  "client_id": "client-5000"  # optional
}

Response 200 (message available):
{
  "message": "Process data"
}

Response 200 (queue empty):
{
  "message": null
}
```

## 3.4 Deployment Guide

**Deployment:**

```bash
docker-compose -f docker/docker-compose.queue.yml up --build
```

---
# 4. Youtube Video (Demo Proyek)
```
https://youtu.be/6WQaNUHV3p4
```
