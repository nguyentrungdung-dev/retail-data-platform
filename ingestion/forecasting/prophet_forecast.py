"""
Demand Forecasting với Facebook Prophet.

Train 1 model cho mỗi (product_code), output forecast cho HORIZON ngày tới.
Lưu kết quả vào Postgres bảng raw_forecast_results + raw_forecast_runs.

Pattern reference: ingestion/sources/excel.py + ingestion/loaders/postgres_loader.py

Quy trình mỗi run:
  1. Tạo 1 row trong raw_forecast_runs (status=RUNNING)
  2. Đọc int_demand_history từ Postgres → DataFrame
  3. Loop từng SKU:
       - fit Prophet với holidays VN
       - predict HORIZON ngày
       - lưu vào raw_forecast_results
  4. Cập nhật run record với MAPE trung bình + status=SUCCESS
"""
from __future__ import annotations

import logging
import time
import uuid
import warnings
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Iterator

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from ingestion.config import DB_URL
from ingestion.forecasting.seasonality import vietnamese_holidays

# ─────────────────────────────────────────────────────────────────────────────
# Tắt log noise của Prophet/cmdstanpy để output sạch.
# QUAN TRỌNG: phải set TRƯỚC khi `from prophet import Prophet`, vì Prophet log
# "Importing plotly failed" ở module-level (ngay lúc import).
# ─────────────────────────────────────────────────────────────────────────────
logging.getLogger("prophet").setLevel(logging.WARNING)
logging.getLogger("prophet.plot").setLevel(logging.CRITICAL)   # tắt plotly noise
logging.getLogger("cmdstanpy").setLevel(logging.WARNING)
warnings.filterwarnings("ignore", category=FutureWarning)

try:
    from prophet import Prophet
except ImportError as e:
    raise ImportError(
        "Package `prophet` chưa được cài. Chạy:\n"
        "    pip install prophet>=1.1.5\n"
        "(hoặc rebuild Airflow image với requirements-airflow.txt mới)"
    ) from e

from loguru import logger


class NoDataError(RuntimeError):
    """Raise khi không có data đủ để forecast — KHÔNG nên retry."""
    pass


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────

DEFAULT_HORIZON_DAYS = 90       # Dự báo 90 ngày tới
DEFAULT_TRAIN_WINDOW = 730      # Dùng 2 năm data gần nhất để train
MIN_HISTORY_DAYS = 60           # SKU có < 60 ngày data → skip (Prophet không học được)
INTERVAL_WIDTH = 0.80           # Confidence interval 80%


@dataclass
class ForecastConfig:
    """Tham số chạy job forecast."""
    horizon_days: int = DEFAULT_HORIZON_DAYS
    train_window_days: int = DEFAULT_TRAIN_WINDOW
    min_history_days: int = MIN_HISTORY_DAYS
    interval_width: float = INTERVAL_WIDTH
    sku_filter: list[str] | None = None         # None = forecast tất cả SKU
    seasonality_mode: str = "multiplicative"    # 'multiplicative' tốt hơn cho retail


@dataclass
class RunStats:
    """Stats tổng hợp của 1 lần chạy."""
    run_id: str
    sku_total: int = 0
    sku_succeeded: int = 0
    sku_skipped: int = 0
    sku_failed: int = 0
    mape_values: list[float] = field(default_factory=list)
    rows_inserted: int = 0

    @property
    def avg_mape(self) -> float | None:
        if not self.mape_values:
            return None
        return float(sum(self.mape_values) / len(self.mape_values))


# ─────────────────────────────────────────────────────────────────────────────
# DATABASE HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def _create_run(engine: Engine, cfg: ForecastConfig) -> str:
    """Insert 1 row vào raw_forecast_runs với status=RUNNING. Return run_id."""
    run_id = f"prophet-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO raw_forecast_runs (
                run_id, run_started_at, horizon_days, train_window_days, status
            ) VALUES (
                :run_id, NOW(), :horizon, :train_window, 'RUNNING'
            )
        """), {
            "run_id": run_id,
            "horizon": cfg.horizon_days,
            "train_window": cfg.train_window_days,
        })
    logger.info(f"[forecast] Bắt đầu run {run_id}")
    return run_id


def _finalize_run(
    engine: Engine,
    run_id: str,
    stats: RunStats,
    status: str,
    error_msg: str | None = None,
) -> None:
    """Cập nhật raw_forecast_runs với kết quả cuối."""
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE raw_forecast_runs
            SET run_finished_at = NOW(),
                sku_count       = :sku_count,
                sku_skipped     = :sku_skipped,
                status          = :status,
                error_message   = :error_msg,
                avg_mape        = :avg_mape,
                notes           = :notes
            WHERE run_id = :run_id
        """), {
            "run_id":      run_id,
            "sku_count":   stats.sku_succeeded,
            "sku_skipped": stats.sku_skipped + stats.sku_failed,
            "status":      status,
            "error_msg":   error_msg,
            "avg_mape":    stats.avg_mape,
            "notes":       (
                f"succeeded={stats.sku_succeeded}, "
                f"skipped={stats.sku_skipped}, "
                f"failed={stats.sku_failed}, "
                f"rows_inserted={stats.rows_inserted}"
            ),
        })


def _load_demand_history(
    engine: Engine,
    train_window_days: int,
    sku_filter: list[str] | None = None,
) -> pd.DataFrame:
    """Đọc int_demand_history từ Postgres (do dbt build trước đó)."""
    sku_clause = ""
    params: dict = {"window_days": train_window_days}

    if sku_filter:
        placeholders = ", ".join(f":sku_{i}" for i in range(len(sku_filter)))
        sku_clause = f"AND product_code IN ({placeholders})"
        params.update({f"sku_{i}": code for i, code in enumerate(sku_filter)})

    sql = f"""
        SELECT
            product_code,
            ds,
            qty_sold AS y
        FROM int_demand_history
        WHERE ds >= CURRENT_DATE - INTERVAL ':window_days days'
          {sku_clause}
        ORDER BY product_code, ds
    """
    # SQLAlchemy text() không bind interval literal — cast thủ công
    sql = sql.replace(":window_days", str(int(train_window_days)))
    df = pd.read_sql(text(sql), engine, params=params)
    df["ds"] = pd.to_datetime(df["ds"])
    return df


def _save_forecast_batch(
    engine: Engine,
    run_id: str,
    product_code: str,
    forecast_df: pd.DataFrame,
    history_df: pd.DataFrame,
) -> int:
    """
    Lưu forecast của 1 SKU vào raw_forecast_results.
    forecast_df: từ Prophet model.predict()
    history_df: data thực tế (để gắn actual_qty vào ngày quá khứ)
    """
    # Tạo lookup actual qty
    actual_lookup = dict(zip(history_df["ds"].dt.date, history_df["y"]))

    rows = []
    for _, row in forecast_df.iterrows():
        ds = row["ds"].date()
        is_actual = ds in actual_lookup
        rows.append({
            "run_id":       run_id,
            "product_code": product_code,
            "ds":           ds,
            "yhat":         max(0.0, float(row["yhat"])),       # Không cho âm
            "yhat_lower":   max(0.0, float(row["yhat_lower"])),
            "yhat_upper":   max(0.0, float(row["yhat_upper"])),
            "is_actual":    is_actual,
            "actual_qty":   float(actual_lookup[ds]) if is_actual else None,
        })

    if not rows:
        return 0

    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO raw_forecast_results (
                run_id, product_code, ds, yhat, yhat_lower, yhat_upper,
                is_actual, actual_qty
            ) VALUES (
                :run_id, :product_code, :ds, :yhat, :yhat_lower, :yhat_upper,
                :is_actual, :actual_qty
            )
            ON CONFLICT (run_id, product_code, ds) DO UPDATE SET
                yhat       = EXCLUDED.yhat,
                yhat_lower = EXCLUDED.yhat_lower,
                yhat_upper = EXCLUDED.yhat_upper,
                actual_qty = EXCLUDED.actual_qty
        """), rows)

    return len(rows)


# ─────────────────────────────────────────────────────────────────────────────
# PROPHET TRAINING
# ─────────────────────────────────────────────────────────────────────────────

def _train_one_sku(
    df: pd.DataFrame,
    cfg: ForecastConfig,
    holidays_df: pd.DataFrame,
) -> tuple[pd.DataFrame, float | None]:
    """
    Fit Prophet trên data của 1 SKU và trả về (forecast_df, mape).

    Args:
        df: DataFrame có 2 cột ds, y (chỉ SKU hiện tại)
        cfg: ForecastConfig
        holidays_df: holidays VN

    Returns:
        forecast_df: ngày + ds + yhat + yhat_lower + yhat_upper
        mape: Mean Absolute Percentage Error trên train data (None nếu không tính được)
    """
    model = Prophet(
        holidays=holidays_df,
        seasonality_mode=cfg.seasonality_mode,
        interval_width=cfg.interval_width,
        weekly_seasonality=True,
        yearly_seasonality=True,
        daily_seasonality=False,    # Daily không meaningful với data hàng ngày
        changepoint_prior_scale=0.05,
    )
    # Thêm seasonality custom: monthly cycle (cho retail VN)
    model.add_seasonality(
        name="monthly",
        period=30.5,
        fourier_order=5,
    )

    model.fit(df[["ds", "y"]])

    # Predict: bao gồm cả past (để compute MAPE) và future
    future = model.make_future_dataframe(
        periods=cfg.horizon_days,
        freq="D",
        include_history=True,
    )
    forecast = model.predict(future)

    # MAPE trên train data (chỉ những ngày có y > 0)
    train_pred = forecast[forecast["ds"].isin(df["ds"])][["ds", "yhat"]]
    merged = df.merge(train_pred, on="ds")
    nonzero = merged[merged["y"] > 0]
    mape = None
    if len(nonzero) >= 10:
        mape = float(
            ((nonzero["y"] - nonzero["yhat"]).abs() / nonzero["y"]).mean() * 100
        )

    return forecast[["ds", "yhat", "yhat_lower", "yhat_upper"]], mape


# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY
# ─────────────────────────────────────────────────────────────────────────────

@contextmanager
def _timer(label: str) -> Iterator[None]:
    start = time.time()
    yield
    logger.info(f"[forecast] {label}: {time.time() - start:.1f}s")


def run_forecast(cfg: ForecastConfig | None = None) -> RunStats:
    """
    Chạy 1 batch forecast cho tất cả SKU đủ điều kiện.

    Args:
        cfg: ForecastConfig (None → dùng default)

    Returns:
        RunStats với thông tin tóm tắt run
    """
    cfg = cfg or ForecastConfig()
    engine = create_engine(DB_URL)

    run_id = _create_run(engine, cfg)
    stats = RunStats(run_id=run_id)

    try:
        with _timer("Load history"):
            history = _load_demand_history(
                engine,
                cfg.train_window_days,
                cfg.sku_filter,
            )

        if history.empty:
            # Phân biệt 2 case: bảng không tồn tại vs bảng rỗng (không có data)
            with engine.connect() as conn:
                exists = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_name = 'int_demand_history'
                    )
                """)).scalar()

            if not exists:
                msg = (
                    "Bảng `int_demand_history` chưa tồn tại trong DWH. "
                    "Hãy chạy `dbt build --select intermediate` trước "
                    "(hoặc trigger DAG retail_daily_etl trước retail_weekly_forecast)."
                )
            else:
                msg = (
                    "Bảng `int_demand_history` tồn tại nhưng KHÔNG có row nào "
                    "đủ điều kiện forecast. Nguyên nhân thường gặp:\n"
                    "  1. raw_orders rỗng → chưa ingest data thật\n"
                    "  2. Không SKU nào có ≥ 30 ngày doanh số (filter trong "
                    "int_demand_history.sql)\n"
                    "Kiểm tra: SELECT count(*) FROM raw_orders;"
                )
            raise NoDataError(msg)

        skus = history["product_code"].unique()
        stats.sku_total = len(skus)
        logger.info(f"[forecast] Bắt đầu forecast {len(skus)} SKU")

        holidays_df = vietnamese_holidays()

        for i, sku in enumerate(skus, start=1):
            sku_df = history[history["product_code"] == sku]

            if len(sku_df) < cfg.min_history_days:
                logger.debug(
                    f"[{i}/{len(skus)}] SKIP {sku} "
                    f"(chỉ có {len(sku_df)} ngày, cần ≥ {cfg.min_history_days})"
                )
                stats.sku_skipped += 1
                continue

            try:
                forecast_df, mape = _train_one_sku(sku_df, cfg, holidays_df)

                rows_inserted = _save_forecast_batch(
                    engine, run_id, sku, forecast_df, sku_df
                )
                stats.rows_inserted += rows_inserted
                stats.sku_succeeded += 1

                if mape is not None:
                    stats.mape_values.append(mape)

                if i % 10 == 0 or i == len(skus):
                    avg_mape = stats.avg_mape
                    logger.info(
                        f"[{i}/{len(skus)}] SKUs done. "
                        f"avg MAPE: {avg_mape:.1f}%" if avg_mape else
                        f"[{i}/{len(skus)}] SKUs done."
                    )

            except Exception as e:
                logger.warning(f"[{i}/{len(skus)}] FAIL {sku}: {e}")
                stats.sku_failed += 1

        _finalize_run(engine, run_id, stats, status="SUCCESS")
        logger.info(
            f"[forecast] ✅ Run {run_id} HOÀN THÀNH: "
            f"{stats.sku_succeeded} SKU thành công, "
            f"{stats.sku_skipped} skip, "
            f"{stats.sku_failed} failed, "
            f"{stats.rows_inserted} rows inserted, "
            f"avg MAPE {stats.avg_mape:.1f}%"
            if stats.avg_mape else
            f"[forecast] ✅ Run {run_id} HOÀN THÀNH"
        )
        return stats

    except Exception as e:
        logger.error(f"[forecast] ❌ Run {run_id} FAILED: {e}")
        _finalize_run(engine, run_id, stats, status="FAILED", error_msg=str(e))
        raise


if __name__ == "__main__":
    run_forecast()
