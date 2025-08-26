"""
Async benchmark for the Mini Distributed Job Queue.
"""
import asyncio
import time
import argparse
import httpx

DEFAULT_API = "http://localhost:8000"

async def enqueue_job(client: httpx.AsyncClient, seconds: float, priority: str) -> str:
    r = await client.post("/jobs", json={
        "type": "sleep",
        "payload": {"seconds": seconds},
        "priority": priority
    })
    r.raise_for_status()
    return r.json()["id"]

async def poll_until_done(client: httpx.AsyncClient, job_id: str) -> str:
    while True:
        r = await client.get(f"/jobs/{job_id}")
        if r.status_code == 404:
            await asyncio.sleep(0.05); continue
        r.raise_for_status()
        data = r.json()
        status = data["status"]
        if status in ("succeeded", "dead", "failed"):
            return status
        await asyncio.sleep(0.05)

async def run_benchmark(api: str, jobs: int, concurrency: int, seconds_per_job: float, priority: str):
    limits = httpx.Limits(max_keepalive_connections=concurrency, max_connections=concurrency)
    async with httpx.AsyncClient(base_url=api, timeout=30.0, limits=limits) as client:
        print(f"Benchmark: jobs={jobs}, concurrency={concurrency}, seconds_per_job={seconds_per_job}, priority={priority}")
        print("Enqueuing jobs...")
        sem = asyncio.Semaphore(concurrency)

        async def enqueue_task():
            async with sem:
                return await enqueue_job(client, seconds_per_job, priority)

        t0 = time.time()
        job_ids = await asyncio.gather(*[enqueue_task() for _ in range(jobs)])
        t1 = time.time()
        enqueue_time = t1 - t0
        print(f"Enqueued {jobs} jobs in {enqueue_time:.3f}s ({jobs/enqueue_time:.1f} jobs/sec)")

        print("Waiting for completion...")
        t2 = time.time()
        statuses = await asyncio.gather(*[poll_until_done(client, jid) for jid in job_ids])
        t3 = time.time()
        complete_time = t3 - t2

        total_time = t3 - t0
        succeeded = sum(1 for s in statuses if s == "succeeded")
        failed = jobs - succeeded

        print("\\nResults")
        print("-------")
        print(f"Total wall time           : {total_time:.3f}s")
        print(f"Completion wait time      : {complete_time:.3f}s")
        print(f"Throughput (completed)    : {jobs/total_time:.1f} jobs/sec")
        print(f"Succeeded / Failed        : {succeeded} / {failed}")

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--api", default=DEFAULT_API)
    p.add_argument("--jobs", type=int, default=200)
    p.add_argument("--concurrency", type=int, default=50)
    p.add_argument("--seconds-per-job", type=float, default=0.05)
    p.add_argument("--priority", choices=["high","default","low"], default="default")
    args = p.parse_args()
    asyncio.run(run_benchmark(args.api, args.jobs, args.concurrency, args.seconds_per_job, args.priority))
