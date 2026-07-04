"""
Demo Scenario — Sliding Window Rate Limiter
--------------------------------------------
Configure : 5 requests per 10 seconds per client.

Scenario
--------
Client "alice" sends 8 requests in the first ~3 seconds   → first 5 ALLOWED, last 3 BLOCKED
Alice waits 10 seconds (full window rolls over)
Alice sends 2 more requests                                → both ALLOWED

A second client "bob" runs in parallel to prove per-client isolation.
Bob sends 6 requests while alice is being blocked          → bob's quota is untouched.

Run
---
    python demo/scenario.py
"""

import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.limiter import SlidingWindowRateLimiter

# ── Config (matches the default API config) ──────────────────────────────────
LIMIT = 5
WINDOW = 10  # seconds

GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


def _tag(allowed: bool) -> str:
    if allowed:
        return f"{GREEN}✓ ALLOWED{RESET}"
    return f"{RED}✗ BLOCKED{RESET}"


def _fire(limiter: SlidingWindowRateLimiter, client_id: str, n: int, delay: float = 0.3) -> None:
    """Send *n* requests from *client_id*, each *delay* seconds apart."""
    for i in range(1, n + 1):
        result = limiter.allow(client_id)
        ts = time.strftime("%H:%M:%S")
        tag = _tag(result.allowed)
        extra = (
            f"remaining={result.remaining}"
            if result.allowed
            else f"retry_after={result.retry_after:.2f}s"
        )
        print(f"  [{ts}] {CYAN}{client_id}{RESET} req #{i:>2}  {tag}  ({extra})")
        if delay > 0 and i < n:
            time.sleep(delay)


def main() -> None:
    limiter = SlidingWindowRateLimiter(limit=LIMIT, window_seconds=WINDOW)

    print(f"\n{BOLD}Rate-Limiter Demo  —  {LIMIT} requests / {WINDOW}s per client{RESET}")
    print("=" * 60)

    # ── Phase 1: Alice sends 8 requests quickly (~0.3 s apart) ───────────────
    print(f"\n{BOLD}Phase 1:{RESET} alice fires 8 requests in ~3 seconds")
    _fire(limiter, "alice", 8, delay=0.3)

    # ── Interleaved: Bob fires 6 requests while alice is blocked ─────────────
    print(f"\n{BOLD}Interleaved:{RESET} bob fires 6 requests (separate quota)")
    _fire(limiter, "bob", 6, delay=0.1)

    # ── Phase 2: Wait for alice's window to expire ────────────────────────────
    wait = WINDOW + 0.5          # a little extra to be safe
    print(f"\n{BOLD}Waiting {wait:.1f}s for alice's window to roll over…{RESET}")
    for remaining in range(int(wait), 0, -1):
        print(f"  {YELLOW}{remaining}s remaining…{RESET}", end="\r", flush=True)
        time.sleep(1)
    time.sleep(wait - int(wait))
    print(" " * 30, end="\r")   # clear the countdown line

    # ── Phase 3: Alice sends 2 more requests ─────────────────────────────────
    print(f"\n{BOLD}Phase 3:{RESET} alice fires 2 requests after window reset")
    _fire(limiter, "alice", 2, delay=0.1)

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + "=" * 60)
    print(f"{BOLD}Summary{RESET}")
    alice_stats = limiter.stats("alice")
    bob_stats   = limiter.stats("bob")
    print(f"  alice → {alice_stats}")
    print(f"  bob   → {bob_stats}")
    print()
    print("Expected behaviour (Sliding Window Log):")
    print("  • Phase 1: requests 1-5 ALLOWED, 6-8 BLOCKED")
    print("  • Bob: requests 1-5 ALLOWED, request 6 BLOCKED (independent quota)")
    print("  • Phase 3: requests 1-2 ALLOWED (fresh window)")
    print()


if __name__ == "__main__":
    main()
