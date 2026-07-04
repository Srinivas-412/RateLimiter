"""
Unit tests for SlidingWindowRateLimiter
----------------------------------------
Run:  pytest tests/ -v
"""

import time
import pytest
from unittest.mock import patch

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.limiter import SlidingWindowRateLimiter, RateLimitResult


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def limiter():
    """5 requests per 10-second window."""
    return SlidingWindowRateLimiter(limit=5, window_seconds=10)


# ── 1. Allowed under the limit ────────────────────────────────────────────────

def test_requests_under_limit_are_allowed(limiter):
    """First N requests must all be allowed and remaining must count down."""
    for i in range(5):
        result = limiter.allow("alice")
        assert result.allowed is True, f"Request {i+1} should be allowed"
        assert result.remaining == 5 - i - 1
        assert result.retry_after is None


# ── 2. Blocked over the limit ─────────────────────────────────────────────────

def test_request_over_limit_is_blocked(limiter):
    """The (N+1)-th request within the window must be blocked."""
    for _ in range(5):
        limiter.allow("alice")

    result = limiter.allow("alice")
    assert result.allowed is False
    assert result.remaining == 0
    assert result.retry_after is not None
    assert result.retry_after > 0


def test_remaining_is_zero_when_blocked(limiter):
    for _ in range(5):
        limiter.allow("alice")
    result = limiter.allow("alice")
    assert result.remaining == 0


# ── 3. Window reset / slot freeing ───────────────────────────────────────────

def test_window_reset_allows_requests_again(limiter):
    """After the window expires all slots should be free again."""
    # Use monkeypatching on time.time to avoid sleeping for 10 s in tests.
    base = 1_000_000.0

    with patch("src.limiter.time.time", return_value=base):
        for _ in range(5):
            limiter.allow("alice")
        blocked = limiter.allow("alice")
        assert blocked.allowed is False

    # Advance clock past the window
    with patch("src.limiter.time.time", return_value=base + 10.1):
        result = limiter.allow("alice")
        assert result.allowed is True
        assert result.remaining == 4


def test_partial_window_slide(limiter):
    """
    Only timestamps that have aged out of the window should free slots.
    If we send 5 requests and advance half the window, no new slots have
    opened yet and the next request should still be blocked.
    """
    base = 1_000_000.0

    with patch("src.limiter.time.time", return_value=base):
        for _ in range(5):
            limiter.allow("alice")

    with patch("src.limiter.time.time", return_value=base + 5.0):
        result = limiter.allow("alice")
        assert result.allowed is False


def test_slot_opens_as_oldest_timestamp_expires():
    """
    Send 5 requests.  Advance clock so that exactly ONE timestamp has
    aged out.  The next request must be allowed (1 slot freed).
    """
    lim = SlidingWindowRateLimiter(limit=5, window_seconds=10)
    base = 1_000_000.0

    # Send first request at t=base, then 4 more at t=base+1
    with patch("src.limiter.time.time", return_value=base):
        lim.allow("alice")
    with patch("src.limiter.time.time", return_value=base + 1):
        for _ in range(4):
            lim.allow("alice")

    # Advance to base + 10.01 — the first timestamp (base) has expired,
    # but the next four (base+1) are still within the window.
    with patch("src.limiter.time.time", return_value=base + 10.01):
        result = lim.allow("alice")
        assert result.allowed is True
        assert result.remaining == 0  # 4 old + 1 new = 5, no slots left


# ── 4. Per-client independence ────────────────────────────────────────────────

def test_clients_are_independent(limiter):
    """Exhausting alice's quota must not affect bob."""
    for _ in range(5):
        limiter.allow("alice")

    blocked = limiter.allow("alice")
    assert blocked.allowed is False

    # Bob has a fresh quota
    for _ in range(5):
        result = limiter.allow("bob")
        assert result.allowed is True

    blocked_bob = limiter.allow("bob")
    assert blocked_bob.allowed is False


# ── 5. retry_after accuracy ───────────────────────────────────────────────────

def test_retry_after_is_accurate():
    """retry_after should be ≈ window_seconds when all slots were just used."""
    lim = SlidingWindowRateLimiter(limit=5, window_seconds=10)
    base = 1_000_000.0

    with patch("src.limiter.time.time", return_value=base):
        for _ in range(5):
            lim.allow("alice")

    with patch("src.limiter.time.time", return_value=base + 0.5):
        result = lim.allow("alice")
        assert result.allowed is False
        # Oldest timestamp is at base; window expires at base+10; now=base+0.5
        # retry_after ≈ 9.5 s
        assert 9.0 < result.retry_after <= 10.0


# ── 6. Edge case — limit=1 ────────────────────────────────────────────────────

def test_limit_one():
    """A limit of 1 should block every second request."""
    lim = SlidingWindowRateLimiter(limit=1, window_seconds=10)
    base = 1_000_000.0

    with patch("src.limiter.time.time", return_value=base):
        first = lim.allow("x")
        assert first.allowed is True
        assert first.remaining == 0

        second = lim.allow("x")
        assert second.allowed is False


# ── 7. Edge case — invalid config ────────────────────────────────────────────

def test_invalid_limit_raises():
    with pytest.raises(ValueError):
        SlidingWindowRateLimiter(limit=0, window_seconds=10)


def test_invalid_window_raises():
    with pytest.raises(ValueError):
        SlidingWindowRateLimiter(limit=5, window_seconds=-1)


# ── 8. Reset helper ──────────────────────────────────────────────────────────

def test_reset_clears_client(limiter):
    for _ in range(5):
        limiter.allow("alice")
    assert limiter.allow("alice").allowed is False

    limiter.reset("alice")
    assert limiter.allow("alice").allowed is True


# ── 9. Stats snapshot ────────────────────────────────────────────────────────

def test_stats_returns_correct_count(limiter):
    for _ in range(3):
        limiter.allow("alice")
    stats = limiter.stats("alice")
    assert stats["count"] == 3
    assert stats["remaining"] == 2


def test_stats_unknown_client_returns_full_quota(limiter):
    stats = limiter.stats("nobody")
    assert stats["count"] == 0
    assert stats["remaining"] == 5
