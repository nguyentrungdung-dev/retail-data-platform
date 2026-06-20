"""
Orchestrator: sync 5 entities từ KiotViet về raw_* tables.

Pattern mỗi entity:
    extract → transform → upsert → update _sync_state

Usage:
    # Sync tất cả 5 entities incremental
    python -m ingestion.sources.kiotviet.runner

    # Chỉ sync 1 entity
    python -m ingestion.sources.kiotviet.runner --entities invoices

    # Force full refresh (bỏ qua _sync_state)
    python -m ingestion.sources.kiotviet.runner --full
"""
from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Callable, Sequence

import pandas as pd
from loguru import logger

from ingestion.config import KIOTVIET_CONFIG, kiotviet_is_configured
from ingestion.loaders.postgres_loader import upsert_dataframe
from ingestion.loaders.sync_state import sync_run
from ingestion.sources.kiotviet.client import KiotVietClient
from ingestion.sources.kiotviet.extractors import (
    extract_customers,
    extract_inventory_snapshot,
    extract_invoices,
    extract_orders,
    extract_products,
)
from ingestion.sources.kiotviet.transformers import (
    transform_customers,
    transform_inventory,
    transform_invoices_to_orders,
    transform_orders_to_orders,
    transform_products,
)

SOURCE_SYSTEM = "kiotviet"

# Entities cần sync incremental (có lastModifiedFrom)
INCREMENTAL_ENTITIES = ("invoices", "orders", "products", "customers")

# Entities sync full snapshot mỗi lần (không có lastModifiedFrom)
SNAPSHOT_ENTITIES = ("inventory",)

ALL_ENTITIES = INCREMENTAL_ENTITIES + SNAPSHOT_ENTITIES


@dataclass
class EntityStats:
    """Stats của 1 lần sync entity."""
    entity: str
    rows_fetched: int = 0
    rows_upserted: int = 0
    rows_updated: int = 0
    rows_skipped: int = 0
    error: str | None = None

    @property
    def success(self) -> bool:
        return self.error is None


# ── Sync functions per entity ───────────────────────────────────────

def _sync_invoices(client: KiotVietClient, branch_ids: list[int]) -> EntityStats:
    stats = EntityStats(entity="invoices")
    with sync_run(SOURCE_SYSTEM, "invoices",
                  fallback_lookback_days=KIOTVIET_CONFIG["initial_lookback_days"]) as run:

        raw = extract_invoices(client, run.window_start, branch_ids=branch_ids)
        stats.rows_fetched = len(raw)

        df = transform_invoices_to_orders(raw)
        if df.empty:
            run.set_stats(fetched=stats.rows_fetched, upserted=0)
            return stats

        result = upsert_dataframe(df, "raw_orders", "order_id")
        stats.rows_upserted = result["inserted"]
        stats.rows_updated = result["updated"]
        stats.rows_skipped = result["skipped"]
        run.set_stats(
            fetched=stats.rows_fetched,
            upserted=stats.rows_upserted + stats.rows_updated,
        )
    return stats


def _sync_orders(client: KiotVietClient, branch_ids: list[int]) -> EntityStats:
    stats = EntityStats(entity="orders")
    with sync_run(SOURCE_SYSTEM, "orders",
                  fallback_lookback_days=KIOTVIET_CONFIG["initial_lookback_days"]) as run:

        raw = extract_orders(client, run.window_start, branch_ids=branch_ids)
        stats.rows_fetched = len(raw)

        df = transform_orders_to_orders(raw)
        if df.empty:
            run.set_stats(fetched=stats.rows_fetched, upserted=0)
            return stats

        result = upsert_dataframe(df, "raw_orders", "order_id")
        stats.rows_upserted = result["inserted"]
        stats.rows_updated = result["updated"]
        stats.rows_skipped = result["skipped"]
        run.set_stats(
            fetched=stats.rows_fetched,
            upserted=stats.rows_upserted + stats.rows_updated,
        )
    return stats


def _sync_products(client: KiotVietClient) -> EntityStats:
    stats = EntityStats(entity="products")
    with sync_run(SOURCE_SYSTEM, "products",
                  fallback_lookback_days=KIOTVIET_CONFIG["initial_lookback_days"]) as run:

        raw = extract_products(client, run.window_start, include_inventory=False)
        stats.rows_fetched = len(raw)

        df = transform_products(raw)
        if df.empty:
            run.set_stats(fetched=stats.rows_fetched, upserted=0)
            return stats

        result = upsert_dataframe(df, "raw_products", "product_code")
        stats.rows_upserted = result["inserted"]
        stats.rows_updated = result["updated"]
        stats.rows_skipped = result["skipped"]
        run.set_stats(
            fetched=stats.rows_fetched,
            upserted=stats.rows_upserted + stats.rows_updated,
        )
    return stats


def _sync_customers(client: KiotVietClient) -> EntityStats:
    stats = EntityStats(entity="customers")
    with sync_run(SOURCE_SYSTEM, "customers",
                  fallback_lookback_days=KIOTVIET_CONFIG["initial_lookback_days"]) as run:

        raw = extract_customers(client, run.window_start)
        stats.rows_fetched = len(raw)

        df = transform_customers(raw)
        if df.empty:
            run.set_stats(fetched=stats.rows_fetched, upserted=0)
            return stats

        result = upsert_dataframe(df, "raw_customers", "customer_id")
        stats.rows_upserted = result["inserted"]
        stats.rows_updated = result["updated"]
        stats.rows_skipped = result["skipped"]
        run.set_stats(
            fetched=stats.rows_fetched,
            upserted=stats.rows_upserted + stats.rows_updated,
        )
    return stats


def _sync_inventory(client: KiotVietClient, branch_ids: list[int]) -> EntityStats:
    """
    Inventory không incremental — luôn snapshot full.
    KiotViet không có lastModifiedFrom cho inventory.
    Vẫn dùng sync_run để có audit log + state tracking.
    """
    stats = EntityStats(entity="inventory")
    with sync_run(SOURCE_SYSTEM, "inventory", fallback_lookback_days=1) as run:

        raw = extract_inventory_snapshot(client, branch_ids=branch_ids)
        stats.rows_fetched = len(raw)

        df = transform_inventory(raw)
        if df.empty:
            run.set_stats(fetched=stats.rows_fetched, upserted=0)
            return stats

        # raw_inventory PK composite (snapshot_date, product_code)
        # postgres_loader.upsert_dataframe chỉ support single PK
        # → workaround: dùng INSERT ... ON CONFLICT thẳng
        upserted = _upsert_inventory(df)
        stats.rows_upserted = upserted
        run.set_stats(fetched=stats.rows_fetched, upserted=upserted)
    return stats


def _upsert_inventory(df: pd.DataFrame) -> int:
    """Upsert raw_inventory (composite PK) — không dùng được postgres_loader chuẩn."""
    from sqlalchemy import create_engine, text

    from ingestion.config import DB_URL

    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(text("""
                INSERT INTO raw_inventory (
                    snapshot_date, product_code, qty_on_hand, qty_reserved,
                    warehouse_loc
                ) VALUES (
                    :snapshot_date, :product_code, :qty_on_hand, :qty_reserved,
                    :warehouse_loc
                )
                ON CONFLICT (snapshot_date, product_code) DO UPDATE SET
                    qty_on_hand   = EXCLUDED.qty_on_hand,
                    qty_reserved  = EXCLUDED.qty_reserved,
                    warehouse_loc = EXCLUDED.warehouse_loc
            """), row.to_dict())
    return len(df)


# ── Public entry points ─────────────────────────────────────────────

ENTITY_FUNCTIONS: dict[str, Callable[..., EntityStats]] = {
    "invoices":  _sync_invoices,
    "orders":    _sync_orders,
    "products":  _sync_products,
    "customers": _sync_customers,
    "inventory": _sync_inventory,
}


def sync_entity(entity: str) -> EntityStats:
    """Sync 1 entity duy nhất."""
    if entity not in ENTITY_FUNCTIONS:
        raise ValueError(
            f"Unknown entity '{entity}'. "
            f"Options: {sorted(ENTITY_FUNCTIONS)}"
        )

    if not kiotviet_is_configured():
        raise RuntimeError(
            "KiotViet credentials chưa setup. Xem docs/kiotviet_setup.md"
        )

    client = KiotVietClient()
    branch_ids = KIOTVIET_CONFIG["branch_ids"]

    fn = ENTITY_FUNCTIONS[entity]
    if entity in ("invoices", "orders", "inventory"):
        return fn(client, branch_ids)
    return fn(client)


def sync_all(
    entities: Sequence[str] | None = None,
    full_refresh: bool = False,
) -> list[EntityStats]:
    """
    Sync nhiều entities, return list stats.

    Args:
        entities: list entity name (None = tất cả 5)
        full_refresh: nếu True, reset _sync_state về 1 năm trước rồi mới sync
    """
    if not kiotviet_is_configured():
        logger.error(
            "[runner] KiotViet credentials chưa setup. "
            "Set KIOTVIET_CLIENT_ID, KIOTVIET_CLIENT_SECRET, KIOTVIET_RETAILER. "
            "Xem docs/kiotviet_setup.md"
        )
        return []

    targets = list(entities) if entities else list(ALL_ENTITIES)

    if full_refresh:
        _reset_sync_state(targets)

    logger.info(f"[runner] Sync KiotViet entities: {targets}")
    all_stats: list[EntityStats] = []

    for entity in targets:
        try:
            stats = sync_entity(entity)
            all_stats.append(stats)
        except Exception as e:
            logger.error(f"[runner] {entity} crashed: {e}")
            all_stats.append(EntityStats(entity=entity, error=str(e)))

    # Summary
    total_fetched = sum(s.rows_fetched for s in all_stats)
    total_upserted = sum(s.rows_upserted + s.rows_updated for s in all_stats)
    failed = [s.entity for s in all_stats if not s.success]

    logger.info(
        f"[runner] DONE: {len(targets)} entities, "
        f"fetched={total_fetched}, upserted={total_upserted}, "
        f"failed={failed or 'none'}"
    )
    return all_stats


def _reset_sync_state(entities: Sequence[str]) -> None:
    """Reset _sync_state về 1 năm trước cho các entity (full refresh)."""
    from sqlalchemy import create_engine, text

    from ingestion.config import DB_URL

    one_year_ago = datetime.utcnow() - timedelta(days=365)

    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        for entity in entities:
            conn.execute(text("""
                UPDATE _sync_state
                SET last_synced_at = :ts
                WHERE source_system = :source AND entity_name = :entity
            """), {
                "ts":     one_year_ago,
                "source": SOURCE_SYSTEM,
                "entity": entity,
            })
    logger.warning(
        f"[runner] FULL REFRESH: reset {len(entities)} entities về {one_year_ago}"
    )


# ── CLI ──────────────────────────────────────────────────────────────

def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Sync data từ KiotViet API")
    parser.add_argument(
        "--entities",
        nargs="+",
        choices=sorted(ENTITY_FUNCTIONS),
        default=None,
        help="Entities cần sync (mặc định: tất cả 5)",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Reset sync_state → kéo lùi 1 năm rồi sync (full refresh)",
    )
    args = parser.parse_args(argv)

    stats = sync_all(entities=args.entities, full_refresh=args.full)

    failed_count = sum(1 for s in stats if not s.success)
    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
