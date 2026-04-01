"""
Jira Cloud API rate limiter with automatic retry, backoff, and thread safety.

Wraps requests.Session to transparently handle Jira's point-based rate limiting.
Designed for parallel use by augment.py (10 concurrent workers).

Usage:
    limiter = JiraRateLimiter(base_url="https://your-domain.atlassian.net",
                              email="you@example.com",
                              api_token="your-token")
    response = limiter.get("/rest/api/3/issue/PROJ-1")
    response = limiter.post("/rest/api/3/issue", json=payload)
"""

import logging
import threading
import time
from typing import Any, Optional

import requests

logger = logging.getLogger(__name__)


class RateLimitExhausted(Exception):
    """Raised when all retries are exhausted on a 429 response."""

    def __init__(self, url: str, retries: int, last_retry_after: float):
        self.url = url
        self.retries = retries
        self.last_retry_after = last_retry_after
        super().__init__(
            f"Rate limit exhausted after {retries} retries on {url}. "
            f"Last Retry-After: {last_retry_after}s"
        )


class JiraRateLimiter:
    """Thread-safe Jira Cloud API client with automatic rate-limit handling.

    Jira Cloud uses a point-based rate limit system. Response headers include:
        X-RateLimit-Remaining  -- points left in the current window
        X-RateLimit-Reset      -- epoch time when the window resets
        Retry-After            -- seconds to wait (on 429 responses)

    This class:
        - Retries on 429 with exponential backoff (up to max_retries)
        - Pre-emptively sleeps when remaining points are critically low
        - Logs throttle events and periodic point summaries
        - Exposes counters for callers to checkpoint (resume support)

    Args:
        base_url: Jira Cloud instance URL (e.g. https://your-domain.atlassian.net)
        email: Account email for Basic auth
        api_token: Jira API token for Basic auth
        max_retries: Maximum retries on 429 (default 5)
        low_point_threshold: Sleep preemptively when remaining points drop below
                             this value (default 10)
        log_interval: Log point summary every N requests (default 100)
    """

    def __init__(
        self,
        base_url: str,
        email: str,
        api_token: str,
        max_retries: int = 5,
        low_point_threshold: int = 10,
        log_interval: int = 100,
    ):
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.low_point_threshold = low_point_threshold
        self.log_interval = log_interval

        # Session with Basic auth
        self._session = requests.Session()
        self._session.auth = (email, api_token)
        self._session.headers.update(
            {
                "Accept": "application/json",
                "Content-Type": "application/json",
            }
        )

        # Thread-safe counters
        self._lock = threading.Lock()
        self._requests_made: int = 0
        self._points_consumed: int = 0
        self._retries_total: int = 0
        self._last_remaining: Optional[int] = None
        self._last_reset: Optional[float] = None

    # ------------------------------------------------------------------ #
    # Public counters -- callers use these to checkpoint for resume
    # ------------------------------------------------------------------ #

    @property
    def requests_made(self) -> int:
        with self._lock:
            return self._requests_made

    @property
    def points_consumed(self) -> int:
        with self._lock:
            return self._points_consumed

    @property
    def retries_total(self) -> int:
        with self._lock:
            return self._retries_total

    @property
    def last_remaining(self) -> Optional[int]:
        with self._lock:
            return self._last_remaining

    def stats(self) -> dict:
        """Snapshot of all counters. Safe to call from any thread."""
        with self._lock:
            return {
                "requests_made": self._requests_made,
                "points_consumed": self._points_consumed,
                "retries_total": self._retries_total,
                "last_remaining": self._last_remaining,
                "last_reset": self._last_reset,
            }

    # ------------------------------------------------------------------ #
    # HTTP methods
    # ------------------------------------------------------------------ #

    def get(self, path: str, **kwargs: Any) -> requests.Response:
        """Send a GET request through the rate limiter."""
        return self._request("GET", path, **kwargs)

    def post(self, path: str, **kwargs: Any) -> requests.Response:
        """Send a POST request through the rate limiter."""
        return self._request("POST", path, **kwargs)

    def put(self, path: str, **kwargs: Any) -> requests.Response:
        """Send a PUT request through the rate limiter."""
        return self._request("PUT", path, **kwargs)

    def delete(self, path: str, **kwargs: Any) -> requests.Response:
        """Send a DELETE request through the rate limiter."""
        return self._request("DELETE", path, **kwargs)

    # ------------------------------------------------------------------ #
    # Core request loop with retry
    # ------------------------------------------------------------------ #

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        """Execute an HTTP request with rate-limit handling and retries.

        If the path is a relative URL (starts with /), it's joined with base_url.
        Absolute URLs are passed through unchanged.
        """
        url = path if path.startswith("http") else f"{self.base_url}{path}"

        # Pre-emptive wait if we know points are critically low
        self._preemptive_wait()

        last_retry_after = 0.0
        for attempt in range(self.max_retries + 1):
            response = self._session.request(method, url, **kwargs)

            # Update rate-limit tracking from response headers
            self._update_tracking(response)

            if response.status_code != 429:
                return response

            # --- 429: Rate limited ---
            retry_after = self._parse_retry_after(response)
            last_retry_after = retry_after

            with self._lock:
                self._retries_total += 1

            if attempt == self.max_retries:
                break

            # Back off: use Retry-After header, or exponential fallback
            wait_time = retry_after if retry_after > 0 else min(2 ** attempt, 60)

            logger.warning(
                "Rate limited (429) on %s %s. Attempt %d/%d. "
                "Sleeping %.1fs (Retry-After: %s)",
                method,
                path,
                attempt + 1,
                self.max_retries,
                wait_time,
                retry_after if retry_after > 0 else "not set",
            )

            time.sleep(wait_time)

        # All retries exhausted
        raise RateLimitExhausted(
            url=url, retries=self.max_retries, last_retry_after=last_retry_after
        )

    # ------------------------------------------------------------------ #
    # Rate-limit header parsing and tracking
    # ------------------------------------------------------------------ #

    def _update_tracking(self, response: requests.Response) -> None:
        """Read rate-limit headers and update internal counters."""
        headers = response.headers
        remaining_str = headers.get("X-RateLimit-Remaining")
        reset_str = headers.get("X-RateLimit-Reset")

        should_log = False
        snap = {}
        with self._lock:
            self._requests_made += 1

            if remaining_str is not None:
                try:
                    new_remaining = int(remaining_str)
                except (ValueError, TypeError):
                    new_remaining = None

                if new_remaining is not None:
                    # Estimate points consumed from remaining-points delta
                    if (
                        self._last_remaining is not None
                        and new_remaining < self._last_remaining
                    ):
                        self._points_consumed += (
                            self._last_remaining - new_remaining
                        )
                    else:
                        # First request, or window reset -- assume 1 point
                        self._points_consumed += 1

                    self._last_remaining = new_remaining

            if reset_str is not None:
                try:
                    self._last_reset = float(reset_str)
                except (ValueError, TypeError):
                    pass

            # Periodic logging
            if self._requests_made % self.log_interval == 0:
                should_log = True
                snap = {
                    "requests_made": self._requests_made,
                    "points_consumed": self._points_consumed,
                    "last_remaining": self._last_remaining,
                    "retries_total": self._retries_total,
                }

        if should_log:
            logger.info(
                "Rate limiter checkpoint: %d requests, ~%d points consumed, "
                "%s remaining, %d retries",
                snap["requests_made"],
                snap["points_consumed"],
                snap["last_remaining"]
                if snap["last_remaining"] is not None
                else "unknown",
                snap["retries_total"],
            )

    def _parse_retry_after(self, response: requests.Response) -> float:
        """Extract Retry-After value in seconds from a 429 response.

        Handles both delta-seconds and HTTP-date formats.
        Returns 0 if header is missing or unparseable.
        """
        header = response.headers.get("Retry-After")
        if header is None:
            return 0.0

        # Try as integer/float seconds first (most common for Jira)
        try:
            return float(header)
        except (ValueError, TypeError):
            pass

        # Try as HTTP-date (RFC 7231)
        try:
            from email.utils import parsedate_to_datetime
            import datetime

            target = parsedate_to_datetime(header)
            now = datetime.datetime.now(tz=target.tzinfo)
            delta = (target - now).total_seconds()
            return max(delta, 0.0)
        except Exception:
            pass

        return 0.0

    def _preemptive_wait(self) -> None:
        """Sleep preemptively if remaining points are critically low.

        Instead of waiting for a 429, pause when we know the window is nearly
        exhausted. Reduces contention across parallel workers.
        """
        with self._lock:
            remaining = self._last_remaining
            reset_epoch = self._last_reset

        if remaining is None or remaining > self.low_point_threshold:
            return

        if reset_epoch is None:
            # Don't know when reset happens -- short sleep as safety valve
            wait = 1.0
        else:
            wait = max(reset_epoch - time.time(), 0.0)
            # Cap at 60s to avoid absurd sleeps from clock skew
            wait = min(wait, 60.0)

        if wait > 0:
            logger.info(
                "Pre-emptive rate limit wait: %d points remaining, "
                "sleeping %.1fs until window reset",
                remaining,
                wait,
            )
            time.sleep(wait)

    # ------------------------------------------------------------------ #
    # Cleanup
    # ------------------------------------------------------------------ #

    def close(self) -> None:
        """Close the underlying requests session."""
        self._session.close()

    def __enter__(self) -> "JiraRateLimiter":
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
