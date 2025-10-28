import asyncio
import time
import matplotlib.pyplot as plt
from aiohttp import ClientSession

QUEUE_LEADER = "http://localhost:9002"
QUEUE_PEERS = ["http://localhost:9001", "http://localhost:9000"]

CACHE_NODES = ["http://localhost:7000", "http://localhost:7001", "http://localhost:7002"]

LOCK_NODES = ["http://localhost:8000", "http://localhost:8001", "http://localhost:8002"]

TOTAL_REQUESTS = 100

async def measure_latency_throughput(session, url, payload=None, method="POST"):
    start = time.perf_counter()
    try:
        if method.upper() == "POST":
            async with session.post(url, json=payload) as resp:
                try:
                    await resp.json()
                except:
                    await resp.text()
        elif method.upper() == "GET":
            async with session.get(url) as resp:
                try:
                    await resp.json()
                except:
                    await resp.text()
    except Exception as e:
        print(f"Error calling {url}: {e}")
    end = time.perf_counter()
    return end - start

async def run_benchmark_node(urls, payload_generator=None, method="POST"):
    async with ClientSession() as session:
        tasks = []
        for i, url in enumerate(urls):
            payload = payload_generator(i) if payload_generator else {"message": f"msg_{i}"}
            tasks.append(measure_latency_throughput(session, url, payload, method))
        latencies = await asyncio.gather(*tasks)
    avg_latency = sum(latencies)/len(latencies)
    throughput = len(latencies)/sum(latencies)
    return avg_latency, throughput

async def benchmark_queue():
    print("Benchmarking Queue Node...")
    single_latency, single_throughput = await run_benchmark_node([QUEUE_LEADER]*TOTAL_REQUESTS)
    distributed_latency, distributed_throughput = await run_benchmark_node(QUEUE_PEERS*TOTAL_REQUESTS)
    return (single_latency, single_throughput), (distributed_latency, distributed_throughput)

async def benchmark_cache():
    print("Benchmarking Cache Node...")
    def payload_gen(i):
        return {"key": f"key_{i}", "value": f"value_{i}"}
    single_latency, single_throughput = await run_benchmark_node([CACHE_NODES[0]]*TOTAL_REQUESTS, payload_gen)
    distributed_latency, distributed_throughput = await run_benchmark_node(CACHE_NODES*TOTAL_REQUESTS, payload_gen)
    return (single_latency, single_throughput), (distributed_latency, distributed_throughput)

async def benchmark_lock():
    print("Benchmarking Lock Node...")
    def payload_gen(i):
        return {"resource": f"res_{i}", "lock_type": "exclusive"}
    single_latency, single_throughput = await run_benchmark_node([LOCK_NODES[0]]*TOTAL_REQUESTS, payload_gen)
    distributed_latency, distributed_throughput = await run_benchmark_node(LOCK_NODES*TOTAL_REQUESTS, payload_gen)
    return (single_latency, single_throughput), (distributed_latency, distributed_throughput)

async def main():
    results = {}

    (q_single_lat, q_single_th), (q_dist_lat, q_dist_th) = await benchmark_queue()
    results["Queue"] = {
        "Single-node": (q_single_lat, q_single_th),
        "Distributed": (q_dist_lat, q_dist_th)
    }

    (c_single_lat, c_single_th), (c_dist_lat, c_dist_th) = await benchmark_cache()
    results["Cache"] = {
        "Single-node": (c_single_lat, c_single_th),
        "Distributed": (c_dist_lat, c_dist_th)
    }

    (l_single_lat, l_single_th), (l_dist_lat, l_dist_th) = await benchmark_lock()
    results["Lock"] = {
        "Single-node": (l_single_lat, l_single_th),
        "Distributed": (l_dist_lat, l_dist_th)
    }

    for comp, vals in results.items():
        print(f"\n=== {comp} ===")
        for scenario, (lat, th) in vals.items():
            print(f"{scenario}: Avg Latency = {lat*1000:.2f} ms, Throughput = {th:.2f} ops/sec")

    components = list(results.keys())
    scenarios = ["Single-node", "Distributed"]
    fig, ax1 = plt.subplots(figsize=(14,7))

    plt.rcParams.update({'font.size': 14})

    bar_colors = {
        "Queue": "skyblue",
        "Cache": "lightgreen",
        "Lock": "orange"
    }
    line_colors = {
        "Queue": "blue",
        "Cache": "green",
        "Lock": "red"
    }

    x_labels = []
    x_positions = []
    pos = 0

    for comp in components:
        throughputs = [results[comp][sc][1] for sc in scenarios]
        ax1.bar([pos, pos+1], throughputs, color=bar_colors[comp], label=f"{comp} Throughput")
        x_labels.extend([f"{comp}-Single", f"{comp}-Distributed"])
        x_positions.extend([pos, pos+1])
        pos += 2

    ax1.set_ylabel("Throughput (ops/sec)", fontsize=16)
    ax1.set_xticks(x_positions)
    ax1.set_xticklabels(x_labels, rotation=30, ha="right", fontsize=16)

    ax2 = ax1.twinx()
    pos = 0
    for comp in components:
        latencies = [results[comp][sc][0]*1000 for sc in scenarios]
        ax2.plot([pos, pos+1], latencies, color=line_colors[comp], marker="o", linewidth=2, label=f"{comp} Latency")
        pos += 2

    ax2.set_ylabel("Latency (ms)", fontsize=16)

    ax1.grid(False)
    ax2.grid(False)

    fig.tight_layout()
    plt.title("Performance Analysis: Throughput vs Latency", fontsize=18)
    ax1.legend(loc='upper left', fontsize=12)
    ax2.legend(loc='upper right', fontsize=12)
    plt.show()


if __name__ == "__main__":
    asyncio.run(main())
