"""
Retail Data Platform — Weekly Demand Forecast DAG
Chạy mỗi Chủ Nhật 03:00 AM (sau khi daily ETL đã xong)

Lý do chạy weekly thay vì daily:
  - Prophet chậm: 100 SKU × 30s ≈ 50 phút
  - Forecast 90 ngày không cần update mỗi đêm; mỗi tuần là đủ
  - Daily DAG vẫn rebuild dbt mart_demand_forecast → dashboard luôn fresh

Flow: refresh dbt intermediate → Prophet forecast → rebuild mart → notify
"""
from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# ── DEFAULT ARGS ────────────────────────────────────────────────

default_args = {
    "owner":            "retail",
    "depends_on_past":  False,
    "start_date":       days_ago(1),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=10),
}


# ── TASK FUNCTIONS ──────────────────────────────────────────────

def run_prophet_forecast(**context):
    """Chạy Prophet forecast cho tất cả SKU đủ điều kiện."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    from airflow.exceptions import AirflowFailException
    from loguru import logger
    from ingestion.forecasting.prophet_forecast import (
        ForecastConfig,
        NoDataError,
        run_forecast,
    )

    cfg = ForecastConfig(
        horizon_days=90,
        train_window_days=730,
        min_history_days=60,
    )

    try:
        stats = run_forecast(cfg)
    except NoDataError as e:
        # Data rỗng → không retry, fail ngay với thông báo rõ
        raise AirflowFailException(str(e)) from e

    logger.info(
        f"[weekly_forecast] run_id={stats.run_id} "
        f"succeeded={stats.sku_succeeded} skipped={stats.sku_skipped} "
        f"failed={stats.sku_failed} avg_mape={stats.avg_mape}"
    )

    # Đẩy stats lên xcom để task notify dùng
    context["ti"].xcom_push(key="forecast_stats", value={
        "run_id":       stats.run_id,
        "sku_total":    stats.sku_total,
        "sku_success":  stats.sku_succeeded,
        "sku_skipped":  stats.sku_skipped,
        "sku_failed":   stats.sku_failed,
        "avg_mape":     round(stats.avg_mape, 2) if stats.avg_mape else None,
    })

    # Nếu < 50% SKU thành công → coi như fail
    if stats.sku_total > 0 and stats.sku_succeeded / stats.sku_total < 0.5:
        raise RuntimeError(
            f"Chỉ {stats.sku_succeeded}/{stats.sku_total} SKU forecast thành công"
        )

    return stats.run_id


def _notify_forecast_done(**context):
    """Gửi notification kèm thống kê forecast."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    from loguru import logger
    from dags.utils.alerts import notify_success

    fc_stats = context["ti"].xcom_pull(
        key="forecast_stats",
        task_ids="run_prophet_forecast",
    ) or {}

    logger.info(f"[weekly_forecast] Forecast stats: {fc_stats}")

    # Gắn vào context để alert message hiển thị
    context["forecast_stats"] = fc_stats
    notify_success(context)


def _notify_failure_callback(context):
    """DAG-level on_failure_callback."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from dags.utils.alerts import notify_failure
    notify_failure(context)


# ── DAG DEFINITION ──────────────────────────────────────────────

with DAG(
    dag_id="retail_weekly_forecast",
    description="Retail — Demand forecasting với Prophet, chạy hàng tuần",
    default_args=default_args,
    schedule_interval="0 3 * * 0",     # 03:00 AM mỗi Chủ Nhật
    catchup=False,
    max_active_runs=1,
    tags=["retail", "ml", "weekly", "forecast"],
    on_failure_callback=_notify_failure_callback,
) as dag:

    # ── TASK 1: Build dbt intermediate (đảm bảo int_demand_history mới nhất)
    # Forecast cần input là int_demand_history → phải build trước.
    # Chỉ build intermediate, KHÔNG build mart (mart sẽ build sau khi có forecast).
    t1_dbt_intermediate = BashOperator(
        task_id="dbt_build_intermediate",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "/opt/dbt-venv/bin/dbt deps "
            "--profiles-dir /opt/airflow/dbt_project "
            "--project-dir /opt/airflow/dbt_project && "
            "/opt/dbt-venv/bin/dbt build "
            "--profiles-dir /opt/airflow/dbt_project "
            "--project-dir /opt/airflow/dbt_project "
            "--select intermediate"
        ),
        execution_timeout=timedelta(minutes=15),
    )

    # ── TASK 2: Prophet forecast (heavy lifting)
    t2_forecast = PythonOperator(
        task_id="run_prophet_forecast",
        python_callable=run_prophet_forecast,
        execution_timeout=timedelta(hours=2),  # Cho phép chạy lâu vì có thể 100+ SKU
    )

    # ── TASK 3: Rebuild mart_demand_forecast với data Prophet mới
    t3_dbt_mart = BashOperator(
        task_id="dbt_build_forecast_mart",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "/opt/dbt-venv/bin/dbt build "
            "--profiles-dir /opt/airflow/dbt_project "
            "--project-dir /opt/airflow/dbt_project "
            "--select mart_demand_forecast"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # ── TASK 4: Notify
    t4_notify = PythonOperator(
        task_id="notify_forecast_done",
        python_callable=_notify_forecast_done,
        trigger_rule="all_success",
    )

    t1_dbt_intermediate >> t2_forecast >> t3_dbt_mart >> t4_notify
