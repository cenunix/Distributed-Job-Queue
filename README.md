# Distributed Job Queue (FastAPI + Redis)

- Priority queues (**high/default/low**), scheduled jobs, retries with
  exponential backoff, dead-letter queue
- REST API for enqueue + status
- Prometheus metrics (`/metrics`) + **Grafana dashboard (auto-provisioned)**
- Live dashboard (`/dashboard`) with demo buttons
- Horizontally scalable workers + benchmark script

## Quick Start (Docker) Or use Makefile

```bash
docker compose up --build -d
or
make run
```

###### API: http://localhost:8000

###### Dashboard: http://localhost:8000/dashboard

- Note: This dashboard is for interactive demos (enqueue jobs, watch them
  drain). For production-grade observability, see Grafana
  (http://localhost:3000)

###### Prometheus: http://localhost:9090

- Extra Gauges

  - `queue_size{priority=...}` – live queue depth by priority
  - `queue_scheduled` – scheduled jobs awaiting promotion
  - `queue_deadletter` – dead-letter backlog
  - `job_queue_build_info{version="…",git_commit="…"}` – build metadata (value
    always `1`)

###### Grafana: http://localhost:3000 (admin/admin; dashboard auto-loaded)

###### Metrics: http://localhost:8000/metrics

### Enqueue a job

```bash
curl -X POST http://localhost:8000/jobs   -H 'Content-Type: application/json'   -d '{"type":"echo","payload":{"msg":"hello"}, "priority":"high"}'
```

### Check status

```bash
curl http://localhost:8000/jobs/<JOB_ID>
```

### Schedule a job for later

```bash
curl -X POST http://localhost:8000/jobs   -H 'Content-Type: application/json'   -d '{"type":"sleep","payload":{"seconds":2},"delay_sec":5}'
```

## Visual Dashboard & Demos

Open http://localhost:8000/dashboard to see live queue sizes and recent jobs.
Use the **demo buttons** to enqueue small/medium/large or **complex**
(priorities + delays + failures) batches.

## Throughput Scaling & Benchmark

Scale workers to show horizontal scaling:

```bash
docker compose up -d --scale worker=5
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python scripts/bench.py --jobs 500 --concurrency 50 --seconds-per-job 0.05 --priority high
```

## Observability: Prometheus + Grafana (auto-provisioned)

- Prometheus scrapes `/metrics` every 1s (see `ops/prometheus.yml`).
- Grafana auto-loads a Prometheus datasource and the prebuilt dashboard.

PromQL ideas:

- `job_queue_enqueued_total`
- `sum by (priority) (rate(job_queue_succeeded_total[30s]))`
- `histogram_quantile(0.95, sum(rate(job_queue_latency_seconds_bucket[2m])) by (le, priority))`
