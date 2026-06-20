"""
Sync state management cho incremental ETL.

Mỗi (source_system, entity_name) có 1 row trong bảng `_sync_state` lưu mốc
`last_synced_at`. Lần extract tiếp theo dùng mốc này làm `lastModifiedFrom`.

Pattern:
    >>> with sync_run("kiotviet", "invoices") as run:
    ...     since = run.window_start             # mốc lastModifiedFrom
    ...     rows = extract_invoices(since)
    ...     # ...
    ...     run.set_stats(fetched=len(rows), upserted=len(rows))
    # Auto: cập nhật _sync_state + ghi _sync_history khi exit context
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import Iterator

from loguru import logger
from sqlalchemy import create_engine, text

from ingestion.config import DB_URL


class SyncRun:
    """Thông tin của 1 lần sync — populate trong context manager."""

    def __init__(
        self,
        source_system: str,
        entity_name: str,
        window_start: datetime,
        window_end: datetime,
    ) -> None:
        self.source_system = source_system
        self.entity_name = entity_name
        self.window_start = window_start    # lastModifiedFrom
        self.window_end = window_end        # mốc kết thúc (== now() lúc bắt đầu)
        self.rows_fetched = 0
        self.rows_upserted = 0
        self.error_message: str | None = None

    def set_stats(self, *, fetched: int = 0, upserted: int = 0) -> None:
        self.rows_fetched = fetched
        self.rows_upserted = upserted


def _get_last_synced_at(
    source_system: str,
    entity_name: str,
    fallback_lookback_days: int = 30,
) -> datetime:
    """
    Đọc last_synced_at từ DB. Nếu chưa có → trả về now - lookback_days
    (lần đầu sync sẽ kéo lùi 30 ngày).
    """
    engine = create_engine(DB_URL)
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT last_synced_at
            FROM _sync_state
            WHERE source_system = :source AND entity_name = :entity
        """), {"source": source_system, "entity": entity_name}).fetchone()

    if row and row[0]:
        return row[0]

    fallback = datetime.utcnow() - timedelta(days=fallback_lookback_days)
    logger.info(
        f"[sync_state] {source_system}.{entity_name}: chưa từng sync, "
        f"dùng lookback {fallback_lookback_days} ngày (từ {fallback})"
    )
    return fallback


def _start_history(run: SyncRun) -> int:
    """Insert 1 row RUNNING vào _sync_history, return sync_id để update sau."""
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        result = conn.execute(text("""
            INSERT INTO _sync_history (
                source_system, entity_name,
                started_at, status,
                sync_window_from, sync_window_to
            ) VALUES (
                :source, :entity, NOW(), 'RUNNING', :wfrom, :wto
            )
            RETURNING sync_id
        """), {
            "source": run.source_system,
            "entity": run.entity_name,
            "wfrom":  run.window_start,
            "wto":    run.window_end,
        })
        return result.scalar_one()


def _finalize(run: SyncRun, sync_id: int, status: str) -> None:
    """Update _sync_history + upsert _sync_state khi sync xong."""
    engine = create_engine(DB_URL)

    with engine.begin() as conn:
        # 1. Update _sync_history với kết quả
        conn.execute(text("""
            UPDATE _sync_history SET
                finished_at  = NOW(),
                duration_ms  = EXTRACT(EPOCH FROM (NOW() - started_at)) * 1000,
                status       = :status,
                rows_fetched = :fetched,
                rows_upserted = :upserted,
                error_message = :error
            WHERE sync_id = :sync_id
        """), {
            "sync_id":  sync_id,
            "status":   status,
            "fetched":  run.rows_fetched,
            "upserted": run.rows_upserted,
            "error":    run.error_message,
        })

        # 2. Upsert _sync_state (chỉ advance window_start nếu SUCCESS)
        # Logic: window_start cho lần sau = window_end của lần này
        # → đảm bảo không miss data ở edge case modified_at = window_end
        if status == "SUCCESS":
            conn.execute(text("""
                INSERT INTO _sync_state (
                    source_system, entity_name, last_synced_at, last_run_at,
                    last_run_status, rows_fetched, rows_upserted
                ) VALUES (
                    :source, :entity, :synced_at, NOW(),
                    'SUCCESS', :fetched, :upserted
                )
                ON CONFLICT (source_system, entity_name) DO UPDATE SET
                    last_synced_at  = EXCLUDED.last_synced_at,
                    last_run_at     = EXCLUDED.last_run_at,
                    last_run_status = EXCLUDED.last_run_status,
                    rows_fetched    = EXCLUDED.rows_fetched,
                    rows_upserted   = EXCLUDED.rows_upserted,
                    error_message   = NULL
            """), {
                "source":   run.source_system,
                "entity":   run.entity_name,
                "synced_at": run.window_end,
                "fetched":  run.rows_fetched,
                "upserted": run.rows_upserted,
            })
        else:
            # FAILED: chỉ update status + error, KHÔNG advance last_synced_at
            # → lần sau retry sẽ pickup lại từ mốc cũ
            conn.execute(text("""
                INSERT INTO _sync_state (
                    source_system, entity_name, last_synced_at, last_run_at,
                    last_run_status, error_message
                ) VALUES (
                    :source, :entity, :synced_at, NOW(),
                    'FAILED', :error
                )
                ON CONFLICT (source_system, entity_name) DO UPDATE SET
                    last_run_at     = EXCLUDED.last_run_at,
                    last_run_status = 'FAILED',
                    error_message   = EXCLUDED.error_message
            """), {
                "source":   run.source_system,
                "entity":   run.entity_name,
                "synced_at": run.window_start,    # Giữ nguyên window cũ
                "error":    run.error_message,
            })


@contextmanager
def sync_run(
    source_system: str,
    entity_name: str,
    fallback_lookback_days: int = 30,
) -> Iterator[SyncRun]:
    """
    Context manager: tự động manage state + history.

    Inside block, populate `run.rows_fetched/upserted` rồi exit normal → SUCCESS.
    Nếu raise exception → FAILED, KHÔNG advance window (lần sau retry).
    """
    window_start = _get_last_synced_at(
        source_system, entity_name, fallback_lookback_days
    )
    window_end = datetime.utcnow()

    run = SyncRun(source_system, entity_name, window_start, window_end)
    sync_id = _start_history(run)

    logger.info(
        f"[sync] BEGIN {source_system}.{entity_name} "
        f"window=[{window_start} → {window_end}]"
    )

    try:
        yield run
        _finalize(run, sync_id, status="SUCCESS")
        logger.info(
            f"[sync] ✅ DONE {source_system}.{entity_name}: "
            f"fetched={run.rows_fetched}, upserted={run.rows_upserted}"
        )
    except Exception as e:
        run.error_message = f"{type(e).__name__}: {e}"
        _finalize(run, sync_id, status="FAILED")
        logger.error(
            f"[sync] ❌ FAIL {source_system}.{entity_name}: {run.error_message}"
        )
        raise
