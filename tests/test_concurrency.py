"""
Concurrency tests for SlidingWindowRateLimiter
-----------------------------------------------
These tests verify that the atomic check-and-append inside allow() prevents
over-admitting requests when many threads call allow() simultaneously.

Run:  pytest tests/test_concurrency.py -v
"""

import threading
import time
from unittest.mock import patch

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.limiter import SlidingWindowRateLimiter


def _concurrent_allow(limiter, client_id: str, n_threads: int) -> list[bool]:
    """
    Fire *n_threads* simultaneous calls to limiter.allow(client_id).
    Returns a list of booleans (True = allowed).
    """
    results: list[bool] = [False] * n_threads
    barrier = threading.Barrier(n_threads)   # ensure all threads start together

    def task(idx: int):
        barrier.wait()                        # synchronise start
        results[idx] = limiter.allow(client_id).allowed

    threads = [threading.Thread(target=task, args=(i,)) for i in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    return results


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_concurrent_requests_respect_limit():
    """
    Fire 20 simultaneous requests against a limit of 5.
    Exactly 5 must be allowed and 15 must be blocked.
    """
    limiter = SlidingWindowRateLimiter(limit=5, window_seconds=10)
    results = _concurrent_allow(limiter, "concurrent_client", n_threads=20)

    allowed = sum(results)
    blocked = len(results) - allowed

    assert allowed == 5,  f"Expected 5 allowed, got {allowed}"
    assert blocked == 15, f"Expected 15 blocked, got {blocked}"


def test_concurrent_different_clients_do_not_interfere():
    """
    Two clients each fire 5 simultaneous requests.
    Each should get exactly their own 5 allowed and 0 blocked
    (limits are independent).
    """
    limiter = SlidingWindowRateLimiter(limit=5, window_seconds=10)

    alice_results: list[bool] = [False] * 5
    bob_results:   list[bool] = [False] * 5
    barrier = threading.Barrier(10)

    def task_alice(idx):
        barrier.wait()
        alice_results[idx] = limiter.allow("alice").allowed

    def task_bob(idx):
        barrier.wait()
        bob_results[idx] = limiter.allow("bob").allowed

    threads = (
        [threading.Thread(target=task_alice, args=(i,)) for i in range(5)]
        + [threading.Thread(target=task_bob, args=(i,)) for i in range(5)]
    )
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sum(alice_results) == 5, f"alice: {alice_results}"
    assert sum(bob_results)   == 5, f"bob: {bob_results}"


def test_no_race_condition_over_many_iterations():
    """
    Repeat the burst-of-20 test 50 times with fresh limiters to catch
    non-deterministic race conditions.
    """
    for run in range(50):
        limiter = SlidingWindowRateLimiter(limit=5, window_seconds=10)
        results = _concurrent_allow(limiter, "stress", n_threads=20)
        allowed = sum(results)
        assert allowed == 5, (
            f"Run {run}: expected exactly 5 allowed, got {allowed} — "
            "possible race condition!"
        )
