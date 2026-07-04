"""
FastAPI demo application
-------------------------
Every request must supply a  ?client_id=<str>  query parameter (acts as
the API-key / user-id for demo purposes).

Endpoints
---------
GET  /ping                  – health check (not rate-limited)
GET  /api/data              – rate-limited demo endpoint
GET  /api/stats/{client_id} – current usage snapshot for a client
POST /api/reset/{client_id} – clear a client's window (admin / test helper)

Responses when rate-limited (HTTP 429)
--------------------------------------
{
  "detail": "Rate limit exceeded",
  "retry_after": 4.217,
  "limit": 5,
  "window_seconds": 10
}

Headers on every /api/data response
------------------------------------
X-RateLimit-Limit     : N
X-RateLimit-Remaining : n
X-RateLimit-Window    : T  (seconds)
Retry-After           : <seconds>  (only on 429)
"""

import os
import sys

# Allow running with  python api/app.py  from the repo root.
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi import FastAPI, Query, Request, Response
from fastapi.responses import JSONResponse

from src.limiter import AsyncSlidingWindowRateLimiter

# ── Configuration ────────────────────────────────────────────────────────────
RATE_LIMIT: int = int(os.getenv("RATE_LIMIT", "5"))
WINDOW_SECONDS: int = int(os.getenv("WINDOW_SECONDS", "10"))

# ── Limiter ───────────────────────────────────────────────────────────────────
# In-memory, per-client, asyncio.Lock — safe for concurrent async requests.
# Change RATE_LIMIT / WINDOW_SECONDS via environment variables at startup.
limiter = AsyncSlidingWindowRateLimiter(limit=RATE_LIMIT, window_seconds=WINDOW_SECONDS)

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Rate Limiter Demo",
    description=f"Sliding-window-log rate limiter — {RATE_LIMIT} req / {WINDOW_SECONDS}s per client.",
    version="1.0.0",
)


# ── Helper ────────────────────────────────────────────────────────────────────

def _add_ratelimit_headers(response: Response, remaining: int, retry_after=None) -> None:
    response.headers["X-RateLimit-Limit"] = str(RATE_LIMIT)
    response.headers["X-RateLimit-Remaining"] = str(remaining)
    response.headers["X-RateLimit-Window"] = str(WINDOW_SECONDS)
    if retry_after is not None:
        response.headers["Retry-After"] = str(retry_after)


# ── Routes ────────────────────────────────────────────────────────────────────

@app.get("/ping", tags=["health"])
async def ping():
    """Simple health-check – always 200."""
    return {"status": "ok", "limit": RATE_LIMIT, "window_seconds": WINDOW_SECONDS}


@app.get("/api/data", tags=["demo"])
async def get_data(
    request: Request,
    response: Response,
    client_id: str = Query(..., description="Unique client identifier (user id / API key / IP)"),
):
    """
    Rate-limited endpoint (async).
    Awaits AsyncSlidingWindowRateLimiter.allow() — the asyncio.Lock inside
    suspends this coroutine if contended, freeing the event loop for others.
    Returns 200 when within quota, 429 when exceeded.
    """
    result = await limiter.allow(client_id)   # ← non-blocking await

    if result.allowed:
        _add_ratelimit_headers(response, result.remaining)
        return {
            "message": f"Hello, {client_id}! Request allowed.",
            "remaining": result.remaining,
            "limit": result.limit,
            "window_seconds": result.window_seconds,
        }

    # Build a 429 response manually so we can attach headers.
    content = {
        "detail": "Rate limit exceeded",
        "retry_after": result.retry_after,
        "limit": result.limit,
        "window_seconds": result.window_seconds,
    }
    headers = {
        "X-RateLimit-Limit": str(RATE_LIMIT),
        "X-RateLimit-Remaining": "0",
        "X-RateLimit-Window": str(WINDOW_SECONDS),
        "Retry-After": str(result.retry_after),
    }
    return JSONResponse(status_code=429, content=content, headers=headers)


@app.get("/api/stats/{client_id}", tags=["admin"])
async def get_stats(client_id: str):
    """Return current usage snapshot for a client (does not consume quota)."""
    return await limiter.stats(client_id)


@app.post("/api/reset/{client_id}", tags=["admin"])
async def reset_client(client_id: str):
    """Clear all recorded timestamps for a client (useful for testing)."""
    await limiter.reset(client_id)
    return {"message": f"Quota reset for {client_id}"}


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("api.app:app", host="0.0.0.0", port=8000, reload=True)
