from typing import Optional, Dict, Any, Literal
from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field
import uuid
import asyncio
import time

from common.queue import (
    get_redis,
    Job,
    enqueue,
    load_job,
    move_due_jobs,
    QUEUES,
    SCHEDULED_ZSET,
    DEAD_LETTER,
    JOB_PREFIX,
)
from common.metrics import inc_counter, render_prometheus, observe_histogram

app = FastAPI(title="Mini Distributed Job Queue", version="0.3.0")

Priority = Literal["high", "default", "low"]


class EnqueueRequest(BaseModel):
    type: Literal["echo", "sleep"] = Field(..., description="Task type")
    payload: Dict[str, Any] = Field(default_factory=dict)
    delay_sec: float = Field(
        0.0, ge=0.0, description="Schedule job to run in N seconds"
    )
    max_retries: int = Field(3, ge=0)
    backoff_sec: float = Field(
        1.5, gt=1.0, description="Exponential base for retry backoff"
    )
    priority: Priority = Field(
        "default", description="Job priority: high, default, or low"
    )


class EnqueueResponse(BaseModel):
    id: str
    status: str
    priority: Priority


class JobStatusResponse(BaseModel):
    id: str
    status: str
    attempts: int
    result: Optional[Any] = None
    error: Optional[str] = None
    priority: Priority


@app.on_event("startup")
async def startup():
    await get_redis()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/jobs", response_model=EnqueueResponse)
async def create_job(req: EnqueueRequest):
    r = await get_redis()
    job = Job(
        id=str(uuid.uuid4()),
        type=req.type,
        payload=req.payload,
        max_retries=req.max_retries,
        backoff_sec=req.backoff_sec,
        priority=req.priority,
    )
    await enqueue(r, job, delay_sec=req.delay_sec)
    await inc_counter(r, "job_queue_enqueued_total", {"priority": job.priority})
    return EnqueueResponse(id=job.id, status=job.status, priority=job.priority)


@app.get("/jobs/{job_id}", response_model=JobStatusResponse)
async def get_job(job_id: str):
    r = await get_redis()
    job = await load_job(r, job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return JobStatusResponse(
        id=job.id,
        status=job.status,
        attempts=job.attempts,
        result=job.result,
        error=job.error,
        priority=job.priority,
    )


@app.post("/_tick")
async def tick():
    r = await get_redis()
    moved = await move_due_jobs(r)
    return {"moved": moved}


@app.get("/metrics")
async def metrics():
    r = await get_redis()
    text = await render_prometheus(r)
    return Response(content=text, media_type="text/plain")


@app.get("/queues")
async def queues_summary():
    r = await get_redis()
    data = {}
    for prio, qname in QUEUES.items():
        data[prio] = await r.llen(qname)
    data["scheduled"] = await r.zcard(SCHEDULED_ZSET)
    data["deadletter"] = await r.llen(DEAD_LETTER)

    peek = {}
    for prio, qname in QUEUES.items():
        ids = await r.lrange(qname, -10, -1)
        peek[prio] = ids
    peek["deadletter"] = await r.lrange(DEAD_LETTER, 0, 9)

    return JSONResponse({"sizes": data, "peek": peek})


@app.get("/", response_class=HTMLResponse)
@app.get("/dashboard", response_class=HTMLResponse)
async def dashboard():
    html = """
<!doctype html>
<html>
<head>
  <meta charset="utf-8">
  <title>Mini Queue Dashboard</title>
  <style>
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Helvetica, Arial, sans-serif; margin: 20px; }
    .cards { display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; margin-bottom: 20px; }
    .card { padding: 12px; border: 1px solid #ddd; border-radius: 10px; box-shadow: 0 1px 3px rgba(0,0,0,0.05); }
    h1 { margin: 0 0 10px; }
    table { border-collapse: collapse; width: 100%; }
    th, td { padding: 6px 8px; border-bottom: 1px solid #eee; font-size: 14px; }
    th { text-align: left; }
    .btns a { display:inline-block; padding:8px 10px; border:1px solid #333; border-radius:8px; text-decoration:none; margin-right:8px; }
    .muted { color: #666; font-size: 12px; }
  </style>
</head>
<body>
  <h1>Mini Distributed Job Queue</h1>

  <div class="cards">
    <div class="card"><b>High</b><div id="high" style="font-size:24px">0</div></div>
    <div class="card"><b>Default</b><div id="default" style="font-size:24px">0</div></div>
    <div class="card"><b>Low</b><div id="low" style="font-size:24px">0</div></div>
    <div class="card"><b>Scheduled</b><div id="scheduled" style="font-size:24px">0</div></div>
    <div class="card"><b>Dead-letter</b><div id="deadletter" style="font-size:24px">0</div></div>
  </div>

  <div class="btns" style="margin-bottom:12px;">
    <a href="#" onclick="runDemo('small'); return false;">Run small demo (12 jobs)</a>
    <a href="#" onclick="runDemo('medium'); return false;">Run medium demo (120 jobs)</a>
    <a href="#" onclick="runDemo('large'); return false;">Run large demo (1000 jobs)</a>
    <a href="#" onclick="runComplex(); return false;">Run complex demo (priorities + delays + failures)</a>
    <a href="/metrics" target="_blank">/metrics</a>
  </div>

  <div class="muted">Auto-updates every 1s without full page reload.</div>

  <h2>Recent jobs</h2>
  <table>
    <thead><tr><th>ID</th><th>Type</th><th>Priority</th><th>Status</th><th>Attempts</th><th>Error</th><th>Updated</th></tr></thead>
    <tbody id="recent-body"></tbody>
  </table>

<script>
async function refresh() {
  try {
    const q = await fetch('/queues').then(r => r.json());
    const s = q.sizes || {};
    for (const k of ['high','default','low']) {
      document.getElementById(k).textContent = s[k] ?? 0;
    }
    document.getElementById('scheduled').textContent = s['scheduled'] ?? 0;
    document.getElementById('deadletter').textContent = s['deadletter'] ?? 0;

    const rj = await fetch('/recent').then(r => r.json());
    const tbody = document.getElementById('recent-body');
    tbody.innerHTML = '';
    for (const j of rj.recent || []) {
      const tr = document.createElement('tr');
      function td(txt){ const t=document.createElement('td'); t.textContent = txt ?? ''; return t; }
      tr.appendChild(td(j.id));
      tr.appendChild(td(j.type));
      tr.appendChild(td(j.priority));
      tr.appendChild(td(j.status));
      tr.appendChild(td(j.attempts));
      tr.appendChild(td(j.error));
      tr.appendChild(td(new Date((parseFloat(j.updated_at)||0)*1000).toLocaleTimeString()));
      tbody.appendChild(tr);
    }
  } catch (e) {
    console.error(e);
  }
}
async function runDemo(size) {
  await fetch(`/demo?size=${encodeURIComponent(size)}`).then(r => r.text());
  setTimeout(refresh, 200);
}
async function runComplex() {
  await fetch(`/demo/complex`).then(r => r.text());
  setTimeout(refresh, 200);
}
refresh();
setInterval(refresh, 1000);
</script>
</body>
</html>
"""
    return HTMLResponse(html)


@app.get("/recent")
async def recent_jobs():
    r = await get_redis()
    keys = await r.keys(JOB_PREFIX + "*")
    jobs = []
    for k in keys[:800]:
        j = await r.hgetall(k)
        jobs.append(j)

    def pf(x, d=0.0):
        try:
            return float(x)
        except:
            return d

    jobs.sort(key=lambda j: pf(j.get("updated_at", "0")), reverse=True)
    recent = []
    for j in jobs[:50]:
        recent.append(
            {
                "id": j.get("id", ""),
                "type": j.get("type", ""),
                "priority": j.get("priority", ""),
                "status": j.get("status", ""),
                "attempts": j.get("attempts", ""),
                "error": j.get("error", ""),
                "updated_at": j.get("updated_at", "0"),
            }
        )
    return JSONResponse({"recent": recent})


@app.get("/demo")
async def demo(size: str = "small"):
    r = await get_redis()
    import uuid as _uuid

    sizes = {"small": 12, "medium": 120, "large": 1000}
    n = sizes.get(size, 12)
    prios = ["high", "default", "low"]
    for i in range(n):
        prio = prios[i % 3]
        job = Job(
            id=str(_uuid.uuid4()),
            type="sleep",
            payload={"seconds": 0.05},
            priority=prio,
        )
        await enqueue(r, job, 0.0)
        await inc_counter(r, "job_queue_enqueued_total", {"priority": job.priority})
    return HTMLResponse(
        f"<p>Enqueued {n} demo jobs (cycled priorities). <a href='/dashboard'>Back</a></p>"
    )


@app.get("/demo/complex")
async def demo_complex():
    r = await get_redis()
    import uuid as _uuid

    for _ in range(10):  # high immediate
        job = Job(
            id=str(_uuid.uuid4()),
            type="sleep",
            payload={"seconds": 0.1},
            priority="high",
        )
        await enqueue(r, job, 0.0)
        await inc_counter(r, "job_queue_enqueued_total", {"priority": job.priority})
    for _ in range(10):  # default delayed
        job = Job(
            id=str(_uuid.uuid4()),
            type="sleep",
            payload={"seconds": 0.1},
            priority="default",
        )
        await enqueue(r, job, 3.0)
        await inc_counter(r, "job_queue_enqueued_total", {"priority": job.priority})
    for _ in range(5):  # low immediate
        job = Job(
            id=str(_uuid.uuid4()),
            type="sleep",
            payload={"seconds": 0.1},
            priority="low",
        )
        await enqueue(r, job, 0.0)
        await inc_counter(r, "job_queue_enqueued_total", {"priority": job.priority})
    for _ in range(3):  # failures to DLQ
        job = Job(
            id=str(_uuid.uuid4()), type="does_not_exist", payload={}, priority="default"
        )
        await enqueue(r, job, 0.0)
        await inc_counter(r, "job_queue_enqueued_total", {"priority": job.priority})
    return HTMLResponse(
        "<p>Enqueued complex demo. Some jobs delayed; a few will fail and go to DLQ. <a href='/dashboard'>Back</a></p>"
    )
