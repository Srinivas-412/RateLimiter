# Rate Limiter — Sliding Window Log

A production-quality, per-client HTTP rate limiter built with Python + FastAPI, using the **Sliding Window Log** algorithm.

---

## Architecture

```
┌────────────────────────────────────────────────────────────────┐
│                      Rate Limiter System                       │
│                                                                │
│  ┌──────────────┐   HTTP    ┌─────────────────────────────┐   │
│  │  HTTP Client │ ────────> │       FastAPI App            │   │
│  └──────────────┘           │  GET /api/data?client_id=x  │   │
│                             │                             │   │
│  ┌──────────────┐           │  RateLimit Dependency       │   │
│  │  Demo Script │           │    calls allow(client_id)   │   │
│  │  (scenario)  │           └──────────────┬──────────────┘   │
│  └──────────────┘                          │                   │
│                             ┌──────────────▼──────────────┐   │
│                             │  SlidingWindowRateLimiter   │   │
│                             │                             │   │
│                             │  allow(client_id)           │   │
│                             │    → RateLimitResult        │   │
│                             │      .allowed               │   │
│                             │      .remaining             │   │
│                             │      .retry_after           │   │
│                             └──────────────┬──────────────┘   │
│                                            │                   │
│                             ┌──────────────▼──────────────┐   │
│                             │    In-Memory Store           │   │
│                             │                             │   │
│                             │  {                          │   │
│                             │    "alice": deque([t1,t2]), │   │
│                             │    "bob":   deque([t3]),    │   │
│                             │  }                          │   │
│                             │                             │   │
│                             │  threading.Lock (global,    │   │
│                             │  atomic check-and-append)   │   │
│                             └─────────────────────────────┘   │
└────────────────────────────────────────────────────────────────┘
```

---

## Algorithm — Sliding Window Log

### How it works

For every client we maintain a `deque` of the **exact UNIX timestamps** of every request that was *allowed* within the rolling window.

On each call to `allow(client_id)`:

1. **Evict** all timestamps older than `now − window_seconds` (they have left the window).
2. **Count** what remains.
3. If `count < limit` → **allow**: append `now`, return `remaining = limit − count − 1`.
4. Otherwise → **block**: compute `retry_after = oldest_timestamp + window_seconds − now`.

```
Timeline (limit=5, window=10s):

t=0   t=1   t=2   t=3   t=10.01
 ●─────●─────●─────●─────          ← requests 1–4 still in window
 │←──────── 10 s ────────→│
 ●─────●─────●────────────●  ✓ t=10.01: oldest (t=0) expired, 1 slot free
```

### Why Sliding Window Log?

| Property | Sliding Window Log | Fixed Window | Sliding Window Counter |
|---|---|---|---|
| Accuracy | **Exact** | Allows 2× burst at boundary | Approximate (good enough) |
| Memory | O(N) per client | O(1) | O(1) |
| Complexity | Low | Lowest | Low |
| Burst at boundary | None | Yes | Minimal |

I chose **Sliding Window Log** because:
- It is **exactly accurate** — no burst at window edges.
- Memory is bounded by `limit` timestamps per client (N = 5 in the demo), which is trivially small.
- The implementation is simple and easy to reason about, test, and audit.

For very high limits (e.g., 10 000 req/s) the Sliding Window Counter would be preferred to save memory.

---

## Trade-offs & Assumptions

| Concern | Decision |
|---|---|
| Storage | In-memory (dict + deque). Fast; lost on restart. Replace with Redis sorted sets for multi-process / distributed deployments. |
| Clock | `time.time()` (wall clock). Susceptible to NTP adjustments; use a monotonic source if needed. |
| Client identity | Caller-supplied `?client_id=` query param. In production this would be extracted from a verified JWT, API key, or IP header. |
| Persistence | Intentionally omitted. Window state is ephemeral. |
| Fairness | All clients share the same global lock. Fine for demo; per-key locks would reduce contention at scale. |

---

## Edge Cases Considered

1. **Simultaneous requests (thundering herd)** — Two threads calling `allow()` at the exact same instant could both pass the `count < limit` check before either appends its timestamp. Solved by holding `threading.Lock` for the entire read-check-write sequence (atomic check-and-append). Verified by the concurrency test suite.

2. **Burst at window boundary (fixed window flaw)** — A fixed-window limiter would allow 2 × N requests straddling a boundary. Sliding Window Log eliminates this entirely because the window always starts from `now`, not from a fixed clock epoch.

3. **`limit = 1`** — The very first request fills the only slot; every subsequent request within the window is blocked. Explicitly tested.

4. **Clock skew / NTP jump** — If the system clock jumps backward, `cutoff = now − window` shrinks, potentially keeping old timestamps longer. Mitigated by using the oldest-timestamp logic for `retry_after` rather than a fixed offset.

5. **Memory growth** — Clients that never get blocked accumulate at most `limit` timestamps each. Stale clients (never seen again) are never evicted; a production system should add an LRU cache or TTL-based eviction.

---

## What I Would Do Differently with More Time

- **Redis backend** — Replace the in-memory dict with a Redis sorted set (`ZADD` / `ZREMRANGEBYSCORE` / `ZCARD`) wrapped in a Lua script for atomic check-and-append. This gives horizontal scalability with no code changes to the `allow()` interface.
- **Per-key locks** — Use a `dict[str, threading.Lock]` to reduce lock contention when many different clients are active simultaneously.
- **Middleware / decorator** — Wrap the limiter in a reusable FastAPI dependency or ASGI middleware that auto-extracts `client_id` from request headers.
- **Prometheus metrics** — Export `rate_limit_allowed_total` and `rate_limit_blocked_total` counters per client for observability dashboards.
- **Graceful degradation** — If the backing store is unavailable, fail open (allow) rather than taking down the service.
- **Adaptive limits** — Allow per-client or per-tier limits loaded from a config/database rather than a single global constant.

---

## Project Structure

```
ratelimter/
├── src/
│   ├── __init__.py
│   └── limiter.py          ← core algorithm (SlidingWindowRateLimiter)
├── api/
│   ├── __init__.py
│   └── app.py              ← FastAPI demo app
├── demo/
│   └── scenario.py         ← Alice / Bob CLI simulation
├── tests/
│   ├── __init__.py
│   ├── test_limiter.py     ← unit tests (10 cases)
│   └── test_concurrency.py ← concurrency / race-condition tests
├── requirements.txt
├── setup_venv.ps1          ← Windows PowerShell setup script
└── README.md
```

---

## Quick Start

### 1 — Create virtual environment & install dependencies

```powershell
# Windows PowerShell
.\setup_venv.ps1
```

Or manually:

```bash
python -m venv .venv
# Windows
.venv\Scripts\activate
# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
```

### 2 — Run the demo scenario (no server needed)

```bash
python demo/scenario.py
```

**Expected output:**
```
Rate-Limiter Demo  —  5 requests / 10s per client
============================================================

Phase 1: alice fires 8 requests in ~3 seconds
  [HH:MM:SS] alice req # 1  ✓ ALLOWED  (remaining=4)
  [HH:MM:SS] alice req # 2  ✓ ALLOWED  (remaining=3)
  [HH:MM:SS] alice req # 3  ✓ ALLOWED  (remaining=2)
  [HH:MM:SS] alice req # 4  ✓ ALLOWED  (remaining=1)
  [HH:MM:SS] alice req # 5  ✓ ALLOWED  (remaining=0)
  [HH:MM:SS] alice req # 6  ✗ BLOCKED  (retry_after=9.11s)
  [HH:MM:SS] alice req # 7  ✗ BLOCKED  (retry_after=8.81s)
  [HH:MM:SS] alice req # 8  ✗ BLOCKED  (retry_after=8.51s)

Interleaved: bob fires 6 requests (separate quota)
  [HH:MM:SS] bob   req # 1  ✓ ALLOWED  (remaining=4)
  ...
  [HH:MM:SS] bob   req # 6  ✗ BLOCKED  (retry_after=9.xx s)

Waiting 10.5s for alice's window to roll over…

Phase 3: alice fires 2 requests after window reset
  [HH:MM:SS] alice req # 1  ✓ ALLOWED  (remaining=4)
  [HH:MM:SS] alice req # 2  ✓ ALLOWED  (remaining=3)
```

### 3 — Start the API server

```bash
uvicorn api.app:app --reload
```

The API defaults to **5 requests / 10 seconds**. Override with environment variables:

```bash
RATE_LIMIT=3 WINDOW_SECONDS=5 uvicorn api.app:app --reload
```

### 4 — Try the API

```bash
# Allowed
curl "http://localhost:8000/api/data?client_id=alice"

# After 5 calls — 429 with Retry-After header
curl -i "http://localhost:8000/api/data?client_id=alice"

# Check usage without consuming quota
curl "http://localhost:8000/api/stats/alice"

# Interactive docs
open http://localhost:8000/docs
```

**200 response:**
```json
{
  "message": "Hello, alice! Request allowed.",
  "remaining": 3,
  "limit": 5,
  "window_seconds": 10
}
```
Headers: `X-RateLimit-Limit: 5`, `X-RateLimit-Remaining: 3`, `X-RateLimit-Window: 10`

**429 response:**
```json
{
  "detail": "Rate limit exceeded",
  "retry_after": 7.43,
  "limit": 5,
  "window_seconds": 10
}
```
Headers: `Retry-After: 7.43`, `X-RateLimit-Remaining: 0`

### 5 — Run the tests

```bash
pytest tests/ -v
```

```
tests/test_limiter.py::test_requests_under_limit_are_allowed     PASSED
tests/test_limiter.py::test_request_over_limit_is_blocked        PASSED
tests/test_limiter.py::test_remaining_is_zero_when_blocked       PASSED
tests/test_limiter.py::test_window_reset_allows_requests_again   PASSED
tests/test_limiter.py::test_partial_window_slide                 PASSED
tests/test_limiter.py::test_slot_opens_as_oldest_timestamp_expires PASSED
tests/test_limiter.py::test_clients_are_independent              PASSED
tests/test_limiter.py::test_retry_after_is_accurate              PASSED
tests/test_limiter.py::test_limit_one                            PASSED
tests/test_limiter.py::test_invalid_limit_raises                 PASSED
tests/test_limiter.py::test_invalid_window_raises                PASSED
tests/test_limiter.py::test_reset_clears_client                  PASSED
tests/test_limiter.py::test_stats_returns_correct_count          PASSED
tests/test_limiter.py::test_stats_unknown_client_returns_full_quota PASSED
tests/test_concurrency.py::test_concurrent_requests_respect_limit     PASSED
tests/test_concurrency.py::test_concurrent_different_clients_do_not_interfere PASSED
tests/test_concurrency.py::test_no_race_condition_over_many_iterations PASSED
```

---

## Concurrency Model

The core risk is a **TOCTOU (Time-of-Check-Time-of-Use)** race:

```
Thread A: reads count=4  (< limit=5) ──────────────────── appends timestamp
Thread B:                   reads count=4  (< limit=5) ──── appends timestamp
                                                          ↑ both allowed — but 6th slot used!
```

**Solution:** wrap the entire `evict → count → check → append` sequence in a single `threading.Lock` acquisition. The lock is released *after* the timestamp is appended, not before. This makes the check-and-write **atomic**.

For distributed systems, the equivalent is a **Redis Lua script** or `MULTI/EXEC` transaction, which runs atomically on a single Redis node.
