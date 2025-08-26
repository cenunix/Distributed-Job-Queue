import asyncio
import time
from typing import Any

from common.queue import (
    get_redis,
    pop_job_id_blocking,
    load_job,
    move_due_jobs,
    mark_succeeded,
    mark_failed_or_retry,
)
from common.metrics import inc_counter, observe_histogram

async def handle_echo(payload: dict) -> Any:
    await asyncio.sleep(0.1)
    return {"echo": payload}

async def handle_sleep(payload: dict) -> Any:
    seconds = float(payload.get("seconds", 1.0))
    await asyncio.sleep(seconds)
    return {"slept": seconds}

TASKS = {
    "echo": handle_echo,
    "sleep": handle_sleep,
}

async def process_one():
    r = await get_redis()
    await move_due_jobs(r)

    popped = await pop_job_id_blocking(r, timeout=2)
    if not popped:
        return False

    qname, job_id = popped
    job = await load_job(r, job_id)
    if not job:
        return False

    print(f"[worker] start id={job.id} type={job.type} prio={job.priority}")
    await inc_counter(r, "job_queue_processed_total", {"priority": job.priority})

    handler = TASKS.get(job.type)
    if not handler:
        await inc_counter(r, "job_queue_failed_total", {"reason": "unknown_task", "priority": job.priority})
        await mark_failed_or_retry(r, job, f"Unknown task type: {job.type}")
        return True

    start = time.time()
    try:
        result = await handler(job.payload)
        await mark_succeeded(r, job, result)
        await inc_counter(r, "job_queue_succeeded_total", {"priority": job.priority})
        latency = time.time() - float(job.created_at)
        await observe_histogram(r, "job_queue_latency_seconds", {"priority": job.priority}, latency)
        print(f"[worker] done  id={job.id} status=succeeded prio={job.priority}")
    except Exception as e:
        await inc_counter(r, "job_queue_failed_total", {"reason": "exception", "priority": job.priority})
        ok = await mark_failed_or_retry(r, job, f"{type(e).__name__}: {e}")
        if ok:
            await inc_counter(r, "job_queue_retries_total", {"priority": job.priority})
    return True

async def main():
    print("Worker started. Polling for jobs...")
    idle_loops = 0
    while True:
        worked = await process_one()
        if not worked:
            idle_loops += 1
            await asyncio.sleep(min(0.5 + idle_loops * 0.05, 2.0))
        else:
            idle_loops = 0

if __name__ == "__main__":
    asyncio.run(main())
