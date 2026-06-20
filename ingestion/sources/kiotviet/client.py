"""
KiotViet HTTP client với retry, pagination, và rate limiting.

Pattern paginated GET của KiotViet:
  GET /<entity>?currentItem=<offset>&pageSize=<n>&lastModifiedFrom=<datetime>
  Response:
    {
      "total":   1234,        ← tổng số rows
      "pageSize": 100,
      "data":    [...]        ← rows trang hiện tại
    }

Rate limit (KiotViet docs): 60 req/phút/retailer. Client tự throttle để không vượt.

Retry policy:
  - 401 → refresh token rồi retry 1 lần
  - 429 (rate limit) → sleep theo Retry-After, retry tối đa 3 lần
  - 5xx → exponential backoff (2s, 4s, 8s), retry 3 lần
  - 4xx khác → raise ngay (không retry, là bug code chứ không phải transient)
"""
from __future__ import annotations

import time
from collections import deque
from typing import Any, Iterator

import requests
from loguru import logger

from ingestion.config import KIOTVIET_CONFIG
from ingestion.sources.kiotviet.auth import (
    KiotVietAuthError,
    TokenManager,
    get_default_token_manager,
)

# ── Constants ────────────────────────────────────────────────────────

_RATE_LIMIT_PER_MINUTE = 55          # Để chừa 5 req buffer dưới giới hạn 60/phút
_MAX_RETRIES_TRANSIENT = 3
_BACKOFF_BASE_SECONDS = 2.0


class KiotVietAPIError(RuntimeError):
    """Lỗi API không retry được (4xx, sai endpoint, schema lệch...)."""
    pass


class KiotVietRateLimiter:
    """
    Rate limiter sliding window 1 phút.
    Đảm bảo không gửi quá N request trong 60 giây.
    """

    def __init__(self, max_per_minute: int = _RATE_LIMIT_PER_MINUTE) -> None:
        self._max = max_per_minute
        self._timestamps: deque[float] = deque()

    def acquire(self) -> None:
        """Block nếu đã đạt rate limit, đến khi có slot mới."""
        now = time.monotonic()

        # Bỏ timestamps cũ hơn 60s
        while self._timestamps and self._timestamps[0] < now - 60:
            self._timestamps.popleft()

        if len(self._timestamps) >= self._max:
            sleep_for = 60 - (now - self._timestamps[0]) + 0.1
            if sleep_for > 0:
                logger.debug(f"[rate-limit] Sleeping {sleep_for:.1f}s")
                time.sleep(sleep_for)

        self._timestamps.append(time.monotonic())


class KiotVietClient:
    """
    HTTP client cho KiotViet Public API.

    Usage:
        >>> client = KiotVietClient()
        >>> for page in client.paginated_get("/invoices",
        ...                                  params={"lastModifiedFrom": "2024-01-01 00:00:00"}):
        ...     for invoice in page["data"]:
        ...         print(invoice["code"])
    """

    def __init__(
        self,
        config: dict | None = None,
        token_manager: TokenManager | None = None,
        rate_limiter: KiotVietRateLimiter | None = None,
    ) -> None:
        self._config = config or KIOTVIET_CONFIG
        self._token_mgr = token_manager or get_default_token_manager()
        self._rate_limiter = rate_limiter or KiotVietRateLimiter()
        self._session = requests.Session()

    # ── Public API ──────────────────────────────────────────────

    def get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Single GET request, returns parsed JSON.
        Tự retry transient errors (401/429/5xx).
        """
        url = self._build_url(path)
        params = params or {}

        last_error: Exception | None = None
        for attempt in range(1, _MAX_RETRIES_TRANSIENT + 2):
            self._rate_limiter.acquire()

            try:
                headers = self._token_mgr.get_auth_headers()
                response = self._session.get(
                    url,
                    headers=headers,
                    params=params,
                    timeout=self._config["request_timeout"],
                )
            except requests.RequestException as e:
                last_error = e
                logger.warning(
                    f"[client] {path} attempt {attempt}: network error {e}"
                )
                self._sleep_backoff(attempt)
                continue

            # 401: token hết hạn → refresh rồi retry 1 lần
            if response.status_code == 401:
                logger.warning(f"[client] {path}: 401 Unauthorized, refreshing token")
                self._token_mgr.invalidate()
                if attempt == 1:
                    continue
                raise KiotVietAuthError(
                    f"401 sau khi refresh token: {response.text[:300]}"
                )

            # 429: rate limit → sleep theo header rồi retry
            if response.status_code == 429:
                retry_after = int(response.headers.get("Retry-After", "10"))
                logger.warning(
                    f"[client] {path}: 429 rate limited, sleep {retry_after}s"
                )
                time.sleep(retry_after)
                continue

            # 5xx: transient server error → backoff
            if 500 <= response.status_code < 600:
                logger.warning(
                    f"[client] {path}: {response.status_code} server error"
                )
                self._sleep_backoff(attempt)
                continue

            # 4xx khác: bug → raise ngay
            if 400 <= response.status_code < 500:
                raise KiotVietAPIError(
                    f"{path} returned {response.status_code}: {response.text[:500]}"
                )

            # 2xx success
            try:
                return response.json()
            except ValueError as e:
                raise KiotVietAPIError(
                    f"{path} response không phải JSON: {response.text[:500]}"
                ) from e

        # Hết retry
        raise KiotVietAPIError(
            f"{path} failed sau {_MAX_RETRIES_TRANSIENT} retries. "
            f"Lỗi cuối: {last_error}"
        )

    def paginated_get(
        self,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """
        Generator yield từng trang response cho đến khi hết data.

        KiotViet pagination: dùng `currentItem` (offset) + `pageSize`.
        Stop khi: trang trả về < pageSize hoặc data rỗng.
        """
        params = dict(params or {})
        page_size = self._config["page_size"]
        params.setdefault("pageSize", page_size)

        offset = 0
        total_seen = 0

        while True:
            params["currentItem"] = offset
            page = self.get(path, params=params)

            data = page.get("data", []) or []
            total_in_response = page.get("total", 0)

            if not data:
                logger.debug(f"[client] {path}: hết data tại offset {offset}")
                break

            yield page

            total_seen += len(data)

            if len(data) < page_size:
                logger.debug(
                    f"[client] {path}: trang cuối ({len(data)} < {page_size}), stop"
                )
                break

            if total_in_response and total_seen >= total_in_response:
                logger.debug(
                    f"[client] {path}: đã lấy đủ {total_seen}/{total_in_response} rows"
                )
                break

            offset += page_size

    # ── Internal ────────────────────────────────────────────────

    def _build_url(self, path: str) -> str:
        base = self._config["base_url"].rstrip("/")
        return f"{base}/{path.lstrip('/')}"

    def _sleep_backoff(self, attempt: int) -> None:
        sleep_for = _BACKOFF_BASE_SECONDS * (2 ** (attempt - 1))
        time.sleep(sleep_for)
