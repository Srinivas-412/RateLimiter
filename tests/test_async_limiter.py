"""
Async concurrency tests for AsyncSlidingWindowRateLimiter
----------------------------------------------------------
These tests fire many coroutines simultaneously (using asyncio.gather)
to verify the asyncio.Lock prevents over-admission.

Run:  pytest tests/test_async_limiter.py -v
"""

import asyncio
from unittest.mock import patch

import pytest
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.limiter import AsyncSlidingWindowRateLimiter


# ── Helper ─────────────────────────────────────────────────────────────────

async def _burst(limiter: AsyncSlidingWindowRateLimiter, client_id: str, n: int) -> list[bool]:
    """Fire n coroutines all at once via asyncio.gather."""
    results = await asyncio.gather(*[limiter.allow(client_id) for _ in range(n)])
    return [r.allowed for r in results]


# ── Tests ───────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_async_allows_up_to_limit():
    """Exactly limit requests must be allowed out of a burst of N > limit."""
    limiter = AsyncSlidingWindowRateLimiter(limit=5, window_seconds=10)
    results = await _burst(limiter, "alice", 20)
    assert sum(results) == 5,  f"Expected 5 allowed, got {sum(results)}"
    assert results.count(False) == 15


@pytest.mark.asyncio
async def test_async_clients_are_independent():
    """Two clients bursting simultaneously must each get their own quota."""
    limiter = AsyncSlidingWindowRateLimiter(limit=5, window_seconds=10)

    alice_tasks = [limiter.allow("alice") for _ in range(5)]
    bob_tasks   = [limiter.allow("bob")   for _ in range(5)]

    all_results = await asyncio.gather(*alice_tasks, *bob_tasks)
    allowed = sum(r.allowed for r in all_results)

    # Both alice and bob should each get 5 slots (10 total)
    assert allowed == 10, f"Expected 10 total allowed (5 each), got {allowed}"


@pytest.mark.asyncio
async def test_async_window_reset():
    """After the window expires the client gets fresh slots."""
    limiter = AsyncSlidingWindowRateLimiter(limit=5, window_seconds=10)
    base = 1_000_000.0

    with patch("src.limiter.time.time", return_value=base):
        results = await _burst(limiter, "alice", 5)
        assert all(results)                             # all 5 allowed
        blocked = await limiter.allow("alice")
        assert blocked.allowed is False

    with patch("src.limiter.time.time", return_value=base + 10.1):
        result = await limiter.allow("alice")
        assert result.allowed is True


@pytest.mark.asyncio
async def test_async_no_race_over_many_iterations():
    """
    Repeat the burst-of-20 test 50 times to catch non-deterministic races
    that asyncio.Lock must prevent.
    """
    for run in range(50):
        limiter = AsyncSlidingWindowRateLimiter(limit=5, window_seconds=10)
        results = await _burst(limiter, "stress", 20)
        allowed = sum(results)
        assert allowed == 5, (
            f"Run {run}: expected exactly 5 allowed, got {allowed} — "
            "possible async race condition!"
        )


@pytest.mark.asyncio
async def test_async_retry_after_is_positive_when_blocked():
    limiter = AsyncSlidingWindowRateLimiter(limit=5, window_seconds=10)
    for _ in range(5):
        await limiter.allow("alice")
    result = await limiter.allow("alice")
    assert result.allowed is False
    assert result.retry_after is not None
    assert result.retry_after > 0


@pytest.mark.asyncio
async def test_async_reset_clears_quota():
    limiter = AsyncSlidingWindowRateLimiter(limit=5, window_seconds=10)
    for _ in range(5):
        await limiter.allow("alice")
    assert (await limiter.allow("alice")).allowed is False

    await limiter.reset("alice")
    assert (await limiter.allow("alice")).allowed is True
