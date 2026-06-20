"""
OAuth2 token management cho KiotViet API.

KiotViet dùng OAuth2 Client Credentials flow:
  POST https://id.kiotviet.vn/connect/token
  Body: grant_type=client_credentials, client_id, client_secret, scopes=PublicApi.Access

Token TTL ~ 24h. Cache trong memory để không refresh mỗi request.
Pattern: chỉ refresh khi sắp hết hạn (< 5 phút) hoặc lần đầu gọi.

Pattern reference: ingestion/loaders/postgres_loader.py (logging + try/except).
"""
from __future__ import annotations

import threading
import time
from dataclasses import dataclass

import requests
from loguru import logger

from ingestion.config import KIOTVIET_CONFIG

# Refresh token sớm 5 phút trước khi hết hạn để tránh race condition
_REFRESH_BUFFER_SECONDS = 300


@dataclass
class CachedToken:
    """Token đang cache cùng metadata expiry."""
    access_token: str
    expires_at: float        # epoch seconds
    token_type: str = "Bearer"

    @property
    def is_valid(self) -> bool:
        return time.time() < (self.expires_at - _REFRESH_BUFFER_SECONDS)

    @property
    def authorization_header(self) -> str:
        return f"{self.token_type} {self.access_token}"


class KiotVietAuthError(RuntimeError):
    """Lỗi authentication — không retry được, cần check credentials."""
    pass


class TokenManager:
    """
    Thread-safe OAuth2 token manager.

    Usage:
        >>> mgr = TokenManager()
        >>> headers = {"Authorization": mgr.get_token().authorization_header}
        >>> # Hoặc tiện hơn:
        >>> headers = mgr.get_auth_headers()
    """

    def __init__(self, config: dict | None = None) -> None:
        self._config = config or KIOTVIET_CONFIG
        self._cached: CachedToken | None = None
        self._lock = threading.Lock()

    # ── Public API ──────────────────────────────────────────────

    def get_token(self, force_refresh: bool = False) -> CachedToken:
        """
        Trả về token còn hiệu lực. Tự động refresh khi sắp hết hạn.

        Args:
            force_refresh: bỏ qua cache, fetch token mới (dùng khi 401).

        Raises:
            KiotVietAuthError: credentials sai hoặc API trả lỗi.
        """
        with self._lock:
            if not force_refresh and self._cached and self._cached.is_valid:
                return self._cached

            self._cached = self._fetch_new_token()
            return self._cached

    def get_auth_headers(self) -> dict[str, str]:
        """
        Trả về headers chuẩn cho mọi request KiotViet.

        Bao gồm:
          - Authorization: Bearer <token>
          - Retailer: <retailer-name>  (header bắt buộc của KiotViet)
        """
        if not self._config["retailer"]:
            raise KiotVietAuthError(
                "KIOTVIET_RETAILER chưa được set trong .env. "
                "Đó là subdomain (vd: 'shopcuaban' nếu URL là shopcuaban.kiotviet.vn)."
            )

        token = self.get_token()
        return {
            "Authorization": token.authorization_header,
            "Retailer":      self._config["retailer"],
        }

    def invalidate(self) -> None:
        """Clear cached token. Gọi khi nhận 401 từ API."""
        with self._lock:
            self._cached = None

    # ── Internal ────────────────────────────────────────────────

    def _fetch_new_token(self) -> CachedToken:
        """Gọi /connect/token để lấy token mới."""
        client_id = self._config["client_id"]
        client_secret = self._config["client_secret"]

        if not client_id or not client_secret:
            raise KiotVietAuthError(
                "Thiếu KIOTVIET_CLIENT_ID hoặc KIOTVIET_CLIENT_SECRET trong .env. "
                "Lấy tại: KiotViet → Cài đặt → API → Tạo ứng dụng."
            )

        logger.info("[kiotviet.auth] Fetching new OAuth2 token...")

        try:
            response = requests.post(
                self._config["token_url"],
                data={
                    "grant_type":    "client_credentials",
                    "client_id":     client_id,
                    "client_secret": client_secret,
                    "scopes":        "PublicApi.Access",
                },
                timeout=self._config["request_timeout"],
            )
        except requests.RequestException as e:
            raise KiotVietAuthError(
                f"Không kết nối được token endpoint: {e}"
            ) from e

        if response.status_code != 200:
            raise KiotVietAuthError(
                f"Token endpoint trả {response.status_code}: {response.text[:500]}"
            )

        try:
            data = response.json()
            access_token = data["access_token"]
            expires_in   = int(data.get("expires_in", 86400))   # default 24h
            token_type   = data.get("token_type", "Bearer")
        except (KeyError, ValueError) as e:
            raise KiotVietAuthError(
                f"Token response sai format: {e}, body={response.text[:500]}"
            ) from e

        token = CachedToken(
            access_token=access_token,
            expires_at=time.time() + expires_in,
            token_type=token_type,
        )

        logger.info(
            f"[kiotviet.auth] ✅ Token lấy thành công, "
            f"expires in {expires_in}s ({expires_in // 3600}h)"
        )
        return token


# Singleton instance — share giữa các module trong cùng process
_default_manager: TokenManager | None = None


def get_default_token_manager() -> TokenManager:
    """Lazy singleton. Test có thể replace bằng mock."""
    global _default_manager
    if _default_manager is None:
        _default_manager = TokenManager()
    return _default_manager
