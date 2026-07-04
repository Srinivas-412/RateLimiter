"""
Sliding Window Log Rate Limiter
--------------------------------
Algorithm
---------
For every client we keep a deque of the exact UNIX timestamps of every
request they were *allowed* within the current window.

On each call to allow(client_id):
  1. Remove (popleft) all timestamps older than  now - window_seconds.
  2. Count what is left.
  3. If count < limit  → allow, append *now*, return allowed=True.
  4. Otherwise         → block, return allowed=False + retry_after.

Thread-safety (sync)
--------------------
threading.Lock wraps the entire evict→count→check→append sequence so the
operation is atomic.  Safe for multi-threaded (sync) servers.

Async-safety
------------
For async servers (FastAPI async def handlers, asyncio), use
AsyncSlidingWindowRateLimiter.  It uses asyncio.Lock which yields control
back to the event loop while waiting — a threading.Lock would block the
entire event loop thread and starve every other coroutine.

Storage / Serialization
------------------------
The store is a plain in-memory dict[str, deque].  No serialization takes
place — values never leave the process.  Atomicity is guaranteed by the
lock, not by serialization.  For multi-process / distributed deployments
replace the dict with a Redis sorted set and a Lua script.
"""

import asyncio
import threading
import time
from collections import deque
from dataclasses import dataclass
from typing import Optional


@dataclass
class RateLimitResult:
    """Return value from SlidingWindowRateLimiter.allow()."""
    allowed: bool
    limit: int
    remaining: int
    window_seconds: int
    retry_after: Optional[float]  # seconds until the oldest slot expires; None when allowed


class SlidingWindowRateLimiter:
    """
    Per-client sliding-window-log rate limiter.

    Parameters
    ----------
    limit          : maximum number of requests allowed per window
    window_seconds : length of the sliding window in seconds

    Example
    -------
    >>> limiter = SlidingWindowRateLimiter(limit=5, window_seconds=10)
    >>> result = limiter.allow("alice")
    >>> result.allowed
    True
    >>> result.remaining
    4
    """

    def __init__(self, limit: int, window_seconds: int) -> None:
        if limit <= 0:
            raise ValueError("limit must be a positive integer")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be a positive integer")

        self.limit = limit
        self.window_seconds = window_seconds

        # { client_id: deque([t1, t2, ...]) }  – only *allowed* timestamps
        self._store: dict[str, deque] = {}
        # One global lock is sufficient for an in-memory store.
        # For finer granularity you could use a per-key lock map.
        self._lock = threading.Lock()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def allow(self, client_id: str) -> RateLimitResult:
        """
        Decide whether the next request from *client_id* should be allowed.

        This method is safe to call from multiple threads concurrently.
        The check-and-append is atomic with respect to other callers for
        the same or different client IDs.

        Returns
        -------
        RateLimitResult
        """
        now = time.time()
        cutoff = now - self.window_seconds

        with self._lock:
            # Lazily initialise the deque for new clients.
            if client_id not in self._store:
                self._store[client_id] = deque()

            timestamps = self._store[client_id]

            # 1. Evict timestamps outside the window.
            while timestamps and timestamps[0] <= cutoff:
                timestamps.popleft()

            count = len(timestamps)

            # 2. Decision.
            if count < self.limit:
                timestamps.append(now)
                return RateLimitResult(
                    allowed=True,
                    limit=self.limit,
                    remaining=self.limit - count - 1,
                    window_seconds=self.window_seconds,
                    retry_after=None,
                )
            else:
                # The oldest timestamp still in the window tells us when the
                # first slot will free up.
                oldest = timestamps[0]
                retry_after = round(oldest + self.window_seconds - now, 3)
                return RateLimitResult(
                    allowed=False,
                    limit=self.limit,
                    remaining=0,
                    window_seconds=self.window_seconds,
                    retry_after=max(retry_after, 0.0),
                )

    def reset(self, client_id: str) -> None:
        """Manually clear all recorded timestamps for a client (useful in tests)."""
        with self._lock:
            self._store.pop(client_id, None)

    def reset_all(self) -> None:
        """Clear the entire store."""
        with self._lock:
            self._store.clear()

    def stats(self, client_id: str) -> dict:
        """Return a snapshot of current usage for *client_id*."""
        now = time.time()
        cutoff = now - self.window_seconds
        with self._lock:
            if client_id not in self._store:
                return {"count": 0, "limit": self.limit, "remaining": self.limit}
            timestamps = self._store[client_id]
            # Count without mutating (eviction happens inside allow())
            count = sum(1 for t in timestamps if t > cutoff)
            return {
                "count": count,
                "limit": self.limit,
                "remaining": max(self.limit - count, 0),
            }


# ---------------------------------------------------------------------------
# Async variant — for FastAPI async def endpoints / asyncio servers
# ---------------------------------------------------------------------------

class AsyncSlidingWindowRateLimiter:
    """
    Async-safe per-client sliding-window-log rate limiter.

    Identical algorithm to SlidingWindowRateLimiter but uses asyncio.Lock
    instead of threading.Lock so it is safe to call with `await` inside
    async def FastAPI handlers without blocking the event loop.

    Why asyncio.Lock and not threading.Lock in async code?
    -------------------------------------------------------
    threading.Lock.acquire() is a *blocking* call.  When called from an
    async coroutine it blocks the entire event loop thread — no other
    coroutine can run until the lock is released.

    asyncio.Lock.__aenter__() is a *suspending* call.  If the lock is
    contended the coroutine is suspended (yields to the event loop), other
    coroutines run, and this one resumes once the lock is free.

    Atomicity
    ---------
    The async lock still wraps the full evict→count→check→append sequence
    so the TOCTOU race (two coroutines both reading count < limit before
    either writes) is prevented.

    Example
    -------
    limiter = AsyncSlidingWindowRateLimiter(limit=5, window_seconds=10)

    @app.get("/api/data")
    async def endpoint(client_id: str):
        result = await limiter.allow(client_id)
        ...
    """

    def __init__(self, limit: int, window_seconds: int) -> None:
        if limit <= 0:
            raise ValueError("limit must be a positive integer")
        if window_seconds <= 0:
            raise ValueError("window_seconds must be a positive integer")

        self.limit = limit
        self.window_seconds = window_seconds

        # Same in-memory store — dict is not serialized, atomicity is the lock's job.
        self._store: dict[str, deque] = {}
        self._lock = asyncio.Lock()

    async def allow(self, client_id: str) -> RateLimitResult:
        """
        Async check-and-admit.  Await this inside async def handlers.

        The lock is held only for the in-memory dict operations (microseconds),
        so contention is minimal even under very high concurrency.
        """
        now = time.time()
        cutoff = now - self.window_seconds

        async with self._lock:          # ← suspends (yields), does NOT block the loop
            if client_id not in self._store:
                self._store[client_id] = deque()

            timestamps = self._store[client_id]

            while timestamps and timestamps[0] <= cutoff:
                timestamps.popleft()

            count = len(timestamps)

            if count < self.limit:
                timestamps.append(now)
                return RateLimitResult(
                    allowed=True,
                    limit=self.limit,
                    remaining=self.limit - count - 1,
                    window_seconds=self.window_seconds,
                    retry_after=None,
                )
            else:
                oldest = timestamps[0]
                retry_after = round(oldest + self.window_seconds - now, 3)
                return RateLimitResult(
                    allowed=False,
                    limit=self.limit,
                    remaining=0,
                    window_seconds=self.window_seconds,
                    retry_after=max(retry_after, 0.0),
                )

    async def reset(self, client_id: str) -> None:
        async with self._lock:
            self._store.pop(client_id, None)

    async def reset_all(self) -> None:
        async with self._lock:
            self._store.clear()

    async def stats(self, client_id: str) -> dict:
        now = time.time()
        cutoff = now - self.window_seconds
        async with self._lock:
            if client_id not in self._store:
                return {"count": 0, "limit": self.limit, "remaining": self.limit}
            timestamps = self._store[client_id]
            count = sum(1 for t in timestamps if t > cutoff)
            return {
                "count": count,
                "limit": self.limit,
                "remaining": max(self.limit - count, 0),
            }
