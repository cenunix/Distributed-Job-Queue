import asyncio
import httpx
import pytest

BASE = "http://localhost:8000"


@pytest.mark.asyncio
async def test_echo_flow():
    async with httpx.AsyncClient(base_url=BASE) as client:
        r = await client.post("/jobs", json={"type": "echo", "payload": {"msg": "hi"}})
        r.raise_for_status()
        job_id = r.json()["id"]
        for _ in range(30):
            s = await client.get(f"/jobs/{job_id}")
            s.raise_for_status()
            if s.json()["status"] == "succeeded":
                assert s.json()["result"]["echo"]["msg"] == "hi"
                return
            await asyncio.sleep(0.2)
        raise AssertionError("Job did not complete in time")


def test_metrics_available():
    r = httpx.get("http://localhost:8000/metrics", timeout=5.0)
    r.raise_for_status()
    body = r.text
    # At least one of our custom gauges + build info should be present
    assert "queue_size" in body
    assert "queue_scheduled" in body
    assert "queue_deadletter" in body
    assert "job_queue_build_info" in body
