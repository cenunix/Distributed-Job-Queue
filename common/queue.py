import os
import json
import time
from dataclasses import dataclass, asdict
from typing import Any, Optional, Dict, Literal, Tuple

import redis.asyncio as redis

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

QUEUES = {
    "high": "queue:high",
    "default": "queue:default",
    "low": "queue:low",
}
PRIORITY_ORDER = ["high", "default", "low"]

SCHEDULED_ZSET = "queue:scheduled"
DEAD_LETTER = "queue:deadletter"
JOB_PREFIX = "job:"

JobStatus = Literal["queued", "scheduled", "processing", "succeeded", "failed", "dead"]
Priority = Literal["high", "default", "low"]

@dataclass
class Job:
    id: str
    type: str
    payload: Dict[str, Any]
    status: JobStatus = "queued"
    attempts: int = 0
    max_retries: int = 3
    backoff_sec: float = 1.5
    next_run_at: Optional[float] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    priority: Priority = "default"
    created_at: float = time.time()
    updated_at: float = time.time()

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

async def get_redis():
    return await redis.from_url(REDIS_URL, encoding="utf-8", decode_responses=True)

def _encode_value(v):
    if v is None:
        return json.dumps(None)  # 'null'
    if isinstance(v, (dict, list)):
        return json.dumps(v)
    return v

async def save_job(r: redis.Redis, job: Job):
    job.updated_at = time.time()
    await r.hset(JOB_PREFIX + job.id, mapping={k: _encode_value(v) for k, v in job.to_dict().items()})
    await r.expire(JOB_PREFIX + job.id, 7 * 24 * 3600)

def _parse(v):
    try:
        return json.loads(v)
    except Exception:
        try:
            if isinstance(v, str) and v.replace(".","",1).isdigit():
                return float(v) if "." in v else int(v)
        except Exception:
            pass
    return v

async def load_job(r: redis.Redis, job_id: str) -> Optional[Job]:
    data = await r.hgetall(JOB_PREFIX + job_id)
    if not data:
        return None
    parsed = {k: _parse(v) for k, v in data.items()}
    return Job(
        id=str(parsed["id"]),
        type=str(parsed["type"]),
        payload=parsed.get("payload") or {},
        status=str(parsed.get("status","queued")),
        attempts=int(parsed.get("attempts",0)),
        max_retries=int(parsed.get("max_retries",3)),
        backoff_sec=float(parsed.get("backoff_sec",1.5)),
        next_run_at=float(parsed["next_run_at"]) if parsed.get("next_run_at") not in (None,"None") else None,
        result=parsed.get("result"),
        error=parsed.get("error"),
        priority=str(parsed.get("priority","default")) if parsed.get("priority") else "default",
        created_at=float(parsed.get("created_at", time.time())),
        updated_at=float(parsed.get("updated_at", time.time())),
    )

async def enqueue(r: redis.Redis, job: Job, delay_sec: float = 0.0):
    if delay_sec and delay_sec > 0:
        job.status = "scheduled"
        job.next_run_at = time.time() + delay_sec
        await save_job(r, job)
        await r.zadd(SCHEDULED_ZSET, {job.id: job.next_run_at})
    else:
        job.status = "queued"
        job.next_run_at = None
        await save_job(r, job)
        qname = QUEUES.get(job.priority, QUEUES["default"])
        await r.lpush(qname, job.id)
    return job

async def move_due_jobs(r: redis.Redis):
    now = time.time()
    due = await r.zrangebyscore(SCHEDULED_ZSET, min=0, max=now, start=0, num=200)
    if not due:
        return 0
    pipe = r.pipeline()
    moved = 0
    for job_id in due:
        prio = await r.hget(JOB_PREFIX + job_id, "priority") or "default"
        qname = QUEUES.get(prio, QUEUES["default"])
        pipe.lpush(qname, job_id)
        pipe.zrem(SCHEDULED_ZSET, job_id)
        pipe.hset(JOB_PREFIX + job_id, mapping={"status": "queued", "updated_at": now, "next_run_at": json.dumps(None)})
        moved += 1
    await pipe.execute()
    return moved

async def pop_job_id_blocking(r: redis.Redis, timeout=5):
    keys = [QUEUES[p] for p in PRIORITY_ORDER]
    res = await r.blpop(keys, timeout=timeout)
    if not res:
        return None
    qname, job_id = res
    return (qname, job_id)

async def mark_succeeded(r: redis.Redis, job: Job, result: Any):
    job.status = "succeeded"
    job.result = result
    job.error = None
    await save_job(r, job)

async def mark_failed_or_retry(r: redis.Redis, job: Job, error: str):
    job.attempts += 1
    if job.attempts > job.max_retries:
        job.status = "dead"
        job.error = error
        await save_job(r, job)
        await r.lpush(DEAD_LETTER, job.id)
        return False
    delay = job.backoff_sec ** job.attempts
    job.status = "scheduled"
    job.next_run_at = time.time() + delay
    job.error = error
    await save_job(r, job)
    await r.zadd(SCHEDULED_ZSET, {job.id: job.next_run_at})
    return True
