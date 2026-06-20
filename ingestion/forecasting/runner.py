"""
Entry point cho job demand forecasting — gọi từ Airflow hoặc chạy thủ công.

Usage:
    # Chạy mặc định (90 ngày, 730 ngày train window)
    python -m ingestion.forecasting.runner

    # Chỉ forecast 1 vài SKU để test
    python -m ingestion.forecasting.runner --skus AC001 AC002 TUL001

    # Custom horizon
    python -m ingestion.forecasting.runner --horizon 30
"""
from __future__ import annotations

import argparse
import sys
from typing import Sequence

from loguru import logger

from ingestion.forecasting.prophet_forecast import (
    DEFAULT_HORIZON_DAYS,
    DEFAULT_TRAIN_WINDOW,
    MIN_HISTORY_DAYS,
    ForecastConfig,
    run_forecast,
)


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Chạy demand forecast với Facebook Prophet",
    )
    parser.add_argument(
        "--horizon",
        type=int,
        default=DEFAULT_HORIZON_DAYS,
        help=f"Số ngày dự báo tới tương lai (default: {DEFAULT_HORIZON_DAYS})",
    )
    parser.add_argument(
        "--train-window",
        type=int,
        default=DEFAULT_TRAIN_WINDOW,
        help=f"Số ngày data dùng để train (default: {DEFAULT_TRAIN_WINDOW})",
    )
    parser.add_argument(
        "--min-history",
        type=int,
        default=MIN_HISTORY_DAYS,
        help=f"Số ngày tối thiểu để forecast 1 SKU (default: {MIN_HISTORY_DAYS})",
    )
    parser.add_argument(
        "--skus",
        nargs="+",
        default=None,
        metavar="SKU",
        help="Filter theo SKU cụ thể (default: tất cả SKU đủ điều kiện)",
    )
    parser.add_argument(
        "--seasonality-mode",
        choices=("multiplicative", "additive"),
        default="multiplicative",
        help="Mode seasonality của Prophet (default: multiplicative)",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)

    cfg = ForecastConfig(
        horizon_days=args.horizon,
        train_window_days=args.train_window,
        min_history_days=args.min_history,
        sku_filter=args.skus,
        seasonality_mode=args.seasonality_mode,
    )

    logger.info(
        f"[runner] Khởi chạy forecast: horizon={cfg.horizon_days}d, "
        f"train_window={cfg.train_window_days}d, "
        f"sku_filter={cfg.sku_filter or 'ALL'}"
    )

    try:
        stats = run_forecast(cfg)
        return 0 if stats.sku_succeeded > 0 else 1
    except Exception as e:
        logger.error(f"[runner] Forecast crashed: {e}")
        return 2


if __name__ == "__main__":
    sys.exit(main())
