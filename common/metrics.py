import time
from typing import Dict, List
import os

import redis.asyncio as redis

DEFAULT_BUCKETS = [0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120]

def encode_labels(labels: Dict[str,str]) -> str:
    if not labels:
        return ""
    return ",".join([f"{k}={v}" for k,v in sorted(labels.items())])

def prom_kv(labels: Dict[str,str]) -> str:
    if not labels:
        return ""
    return "{" + ",".join([f'{k}="{v}"' for k,v in sorted(labels.items())]) + "}"

async def get_redis():
    url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return await redis.from_url(url, encoding="utf-8", decode_responses=True)

async def inc_counter(r: redis.Redis, name: str, labels: Dict[str,str], amount: float = 1.0):
    key = f"metrics:counter:{name}"
    field = encode_labels(labels)
    await r.hincrbyfloat(key, field, float(amount))

async def observe_histogram(r: redis.Redis, name: str, labels: Dict[str,str], value: float, buckets: List[float] = None):
    if buckets is None:
        buckets = DEFAULT_BUCKETS
    buckets = sorted(buckets)
    label_key = encode_labels(labels)
    buck_key = f"metrics:hist:{name}:buckets"
    sum_key = f"metrics:hist:{name}:sum"
    cnt_key = f"metrics:hist:{name}:count"

    pipe = r.pipeline()
    for b in buckets:
        if value <= b:
            field = f"{label_key}|le={b}"
            pipe.hincrbyfloat(buck_key, field, 1.0)
    pipe.hincrbyfloat(buck_key, f"{label_key}|le=+Inf", 1.0)
    pipe.hincrbyfloat(sum_key, label_key, float(value))
    pipe.hincrbyfloat(cnt_key, label_key, 1.0)
    await pipe.execute()

async def render_prometheus(r: redis.Redis) -> str:
    lines = []

    # Counters
    for key in await r.keys("metrics:counter:*"):
        name = key.split("metrics:counter:",1)[1]
        items = await r.hgetall(key)
        lines.append(f"# TYPE {name} counter")
        for field, val in sorted(items.items()):
            labels = {}
            if field:
                for pair in field.split(","):
                    if not pair: continue
                    k,v = pair.split("=",1)
                    labels[k]=v
            lines.append(f"{name}{prom_kv(labels)} {float(val)}")

    # Histograms
    bucket_keys = await r.keys("metrics:hist:*:buckets")
    for buck_key in bucket_keys:
        name = buck_key.split("metrics:hist:",1)[1].split(":buckets",1)[0]
        sum_key = f"metrics:hist:{name}:sum"
        cnt_key = f"metrics:hist:{name}:count"

        buckets = await r.hgetall(buck_key)
        sums = await r.hgetall(sum_key)
        cnts = await r.hgetall(cnt_key)

        lines.append(f"# TYPE {name}_bucket histogram")
        group = {}
        for field, count in buckets.items():
            if "|le=" in field:
                base, le = field.split("|le=",1)
                group.setdefault(base, {})[le] = float(count)

        for base, le_map in sorted(group.items()):
            labels = {}
            if base:
                for pair in base.split(","):
                    if not pair: continue
                    k,v = pair.split("=",1)
                    labels[k]=v

            nums = []
            for k in le_map.keys():
                if k == "+Inf": continue
                try: nums.append(float(k))
                except: pass
            for b in sorted(nums):
                out = dict(labels); out["le"] = str(b)
                lines.append(f"{name}_bucket{prom_kv(out)} {le_map.get(str(b),0.0)}")

            out = dict(labels); out["le"] = "+Inf"
            lines.append(f"{name}_bucket{prom_kv(out)} {le_map.get('+Inf',0.0)}")

            total_count = float(cnts.get(base, 0.0))
            total_sum = float(sums.get(base, 0.0))
            lines.append(f"# TYPE {name}_count counter")
            lines.append(f"{name}_count{prom_kv(labels)} {total_count}")
            lines.append(f"# TYPE {name}_sum counter")
            lines.append(f"{name}_sum{prom_kv(labels)} {total_sum}")

    return "\n".join(lines) + "\n"
