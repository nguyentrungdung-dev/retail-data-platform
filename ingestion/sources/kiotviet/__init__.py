"""
KiotViet API integration package.

Public API:
    >>> from ingestion.sources.kiotviet import KiotVietClient, sync_all
    >>> stats = sync_all()  # Sync tất cả entities incremental

Layout:
    auth.py         OAuth2 token manager (cache 23h)
    client.py       HTTP client (retry, pagination, rate limit)
    extractors.py   Extract raw JSON từ API (per entity)
    transformers.py Map KiotViet schema → raw_* DataFrame schema
    runner.py       Orchestrate sync cả 5 entities
"""
from ingestion.sources.kiotviet.client import KiotVietClient  # noqa: F401
from ingestion.sources.kiotviet.runner import sync_all, sync_entity  # noqa: F401
