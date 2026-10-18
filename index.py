import hashlib
import json
import math
import os
import sys
import time
import uuid
from collections import deque
from datetime import datetime, timezone
from typing import Any, Deque, List, Optional

import httpx
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse

START_TIME = time.time()
OWNER_EMAIL = os.getenv("OWNER_EMAIL", "24f1002494@ds.study.iitm.ac.in")
MAX_LOGS = 500

COUNTER_KEY = "svc:http_requests_total"
LOGS_KEY = "svc:logs"

REDIS_URL = os.getenv("UPSTASH_REDIS_REST_URL") or os.getenv("KV_REST_API_URL")
REDIS_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN") or os.getenv("KV_REST_API_TOKEN")
USE_REDIS = bool(REDIS_URL and REDIS_TOKEN)

app = FastAPI(title="Metrics service")

# ------------------------------------------------------------------ storage
# Shared state lives in Redis so every serverless instance sees one counter and
# one log stream. In-memory fallback is for local runs only.
_local_count = 0
_local_logs: Deque[str] = deque(maxlen=MAX_LOGS)
_http: Optional[httpx.AsyncClient] = None


def _client() -> httpx.AsyncClient:
    global _http
    if _http is None:
        _http = httpx.AsyncClient(timeout=3.0)
    return _http


async def _pipeline(commands: List[list]) -> Optional[list]:
    try:
        resp = await _client().post(
            f"{REDIS_URL.rstrip('/')}/pipeline",
            headers={"Authorization": f"Bearer {REDIS_TOKEN}"},
            json=commands,
        )
        resp.raise_for_status()
        return [item.get("result") for item in resp.json()]
    except Exception as exc:
        print(json.dumps({"level": "error", "msg": f"redis error: {exc}"}), file=sys.stderr)
        return None


async def incr_counter() -> int:
    global _local_count
    if USE_REDIS:
        out = await _pipeline([["INCR", COUNTER_KEY]])
        if out is not None:
            return int(out[0])
    _local_count += 1
    return _local_count


async def get_counter() -> int:
    if USE_REDIS:
        out = await _pipeline([["GET", COUNTER_KEY]])
        if out is not None:
            return int(out[0] or 0)
    return _local_count


async def push_log(entry: dict) -> None:
    line = json.dumps(entry, separators=(",", ":"))
    print(line, flush=True)  # structured JSON to stdout (Vercel runtime logs)
    if USE_REDIS:
        out = await _pipeline([["LPUSH", LOGS_KEY, line], ["LTRIM", LOGS_KEY, 0, MAX_LOGS - 1]])
        if out is not None:
            return
    _local_logs.append(line)


async def read_logs(limit: int) -> List[dict]:
    lines: List[str] = []
    if USE_REDIS:
        out = await _pipeline([["LRANGE", LOGS_KEY, 0, limit - 1]])
        if out is not None:
            lines = list(reversed(out[0] or []))  # stored newest-first -> oldest-first
            return _parse(lines)
    lines = list(_local_logs)[-limit:]
    return _parse(lines)


def _parse(lines: List[str]) -> List[dict]:
    items = []
    for line in lines:
        try:
            items.append(json.loads(line))
        except Exception:
            continue
    return items


# --------------------------------------------------------------- middleware
@app.middleware("http")
async def instrument(request: Request, call_next):
    if request.method == "OPTIONS":
        return await call_next(request)

    request_id = request.headers.get("x-request-id") or uuid.uuid4().hex
    started = time.perf_counter()
    await incr_counter()  # every request to any endpoint counts

    status = 500
    level = "info"
    try:
        response = await call_next(request)
        status = response.status_code
        if status >= 500:
            level = "error"
        elif status >= 400:
            level = "warning"
        response.headers["X-Request-Id"] = request_id
        return response
    except Exception:
        level = "error"
        raise
    finally:
        await push_log(
            {
                "ts": datetime.now(timezone.utc).isoformat(),
                "level": level,
                "msg": "request",
                "request_id": request_id,
                "method": request.method,
                "path": request.url.path,
                "query": request.url.query,
                "status": status,
                "duration_ms": round((time.perf_counter() - started) * 1000, 2),
            }
        )


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["X-Request-Id"],
)


# ---------------------------------------------------------------- endpoints
@app.get("/work")
async def work(n: int = Query(1, ge=0, le=100000)):
    digest = b"seed"
    for _ in range(n):  # K units of CPU work
        digest = hashlib.sha256(digest).digest()
    return {"email": OWNER_EMAIL, "done": n}


@app.get("/metrics", response_class=PlainTextResponse)
async def metrics():
    total = await get_counter()
    body = (
        "# HELP http_requests_total Total HTTP requests received by the service.\n"
        "# TYPE http_requests_total counter\n"
        f"http_requests_total {total}\n"
    )
    return PlainTextResponse(body, media_type="text/plain; version=0.0.4; charset=utf-8")


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "uptime_s": float(max(0.0, time.time() - START_TIME))}


@app.get("/logs/tail")
async def logs_tail(limit: int = Query(50, ge=1, le=MAX_LOGS)):
    return await read_logs(limit)


@app.get("/")
async def root():
    return {"ok": True, "service": "metrics-service"}
