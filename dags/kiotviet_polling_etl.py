"""
Retail Data Platform — KiotViet Realtime Polling ETL (Bước 13)

Chạy mỗi 15 phút để pull data mới từ KiotViet API.
Flow: sync 5 entities incremental → trigger dbt build downstream.

Tần suất 15 phút = đủ "near real-time" cho business analytics, đồng thời:
  - Không vượt rate limit KiotViet (60 req/phút) ngay cả khi data nhiều
  - Đủ để dashboard Metabase refresh kịp
  - Tiết kiệm tài nguyên server

Khi cần real-time đúng nghĩa (< 5s), bổ sung webhook receiver — chưa làm ở đây.
"""
from __future__ import annotations

from datetime import timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner":            "retail",
    "depends_on_past":  False,
    "start_date":       days_ago(1),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=3),
    # Quan trọng: nếu chạy quá lâu (15 phút next run đã đến) → kill task này
    # tránh tình trạng overlap nhiều run cùng lúc bóp DB.
    "execution_timeout": timedelta(minutes=12),
}


# ── TASK FUNCTIONS ──────────────────────────────────────────────

def _check_credentials(**context):
    """Skip toàn DAG nếu chưa cấu hình KiotViet credentials."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    from ingestion.config import kiotviet_is_configured

    if not kiotviet_is_configured():
        raise AirflowSkipException(
            "KiotViet credentials chưa cấu hình (KIOTVIET_CLIENT_ID, "
            "KIOTVIET_CLIENT_SECRET, KIOTVIET_RETAILER trong .env). "
            "Xem docs/kiotviet_setup.md."
        )

    from loguru import logger
    logger.info("[kiotviet] Credentials OK, bắt đầu sync")


def _run_sync(**context):
    """Sync 5 entities từ KiotViet."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    from loguru import logger
    from ingestion.sources.kiotviet.runner import sync_all

    stats = sync_all()

    # Push summary lên xcom cho task notify
    summary = {
        "entities": [
            {
                "entity":         s.entity,
                "rows_fetched":   s.rows_fetched,
                "rows_upserted":  s.rows_upserted + s.rows_updated,
                "success":        s.success,
                "error":          s.error,
            }
            for s in stats
        ],
        "total_fetched": sum(s.rows_fetched for s in stats),
        "total_upserted": sum(s.rows_upserted + s.rows_updated for s in stats),
        "failed_count": sum(1 for s in stats if not s.success),
    }
    context["ti"].xcom_push(key="kiotviet_sync_summary", value=summary)

    logger.info(f"[kiotviet] Sync done: {summary}")

    # Nếu > 50% entities fail → fail task
    failed = summary["failed_count"]
    total = len(stats)
    if total > 0 and failed / total > 0.5:
        raise RuntimeError(
            f"{failed}/{total} entities sync FAILED — kiểm tra log"
        )


def _skip_dbt_if_no_changes(**context):
    """Skip dbt build nếu không có row nào được upsert (đỡ tốn tài nguyên)."""
    summary = context["ti"].xcom_pull(
        key="kiotviet_sync_summary",
        task_ids="run_sync",
    ) or {}

    upserted = summary.get("total_upserted", 0)
    if upserted == 0:
        raise AirflowSkipException(
            "Không có row nào upsert → skip dbt build"
        )

    from loguru import logger
    logger.info(f"[kiotviet] {upserted} rows changed, sẽ trigger dbt build")


def _notify_failure_callback(context):
    """DAG-level on_failure_callback."""
    import sys
    sys.path.insert(0, "/opt/airflow")
    from dags.utils.alerts import notify_failure
    notify_failure(context)


# ── DAG DEFINITION ──────────────────────────────────────────────

with DAG(
    dag_id="retail_kiotviet_polling",
    description="Retail — Sync KiotViet API mỗi 15 phút (incremental)",
    default_args=default_args,
    schedule_interval="*/15 * * * *",     # Mỗi 15 phút
    catchup=False,
    max_active_runs=1,                     # Không cho 2 run chạy song song
    tags=["retail", "kiotviet", "ingestion", "realtime"],
    on_failure_callback=_notify_failure_callback,
) as dag:

    t1_check = PythonOperator(
        task_id="check_credentials",
        python_callable=_check_credentials,
    )

    t2_sync = PythonOperator(
        task_id="run_sync",
        python_callable=_run_sync,
    )

    t3_check_changes = PythonOperator(
        task_id="check_for_changes",
        python_callable=_skip_dbt_if_no_changes,
    )

    # Rebuild staging + intermediate + marts.
    # EXCLUDE mart_demand_forecast: cần Prophet output mới meaningful, được rebuild
    # bởi DAG retail_weekly_forecast (chạy mỗi Chủ Nhật) chứ không phải mỗi 15 phút.
    t4_dbt = BashOperator(
        task_id="dbt_build_downstream",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "/opt/dbt-venv/bin/dbt build "
            "--profiles-dir /opt/airflow/dbt_project "
            "--project-dir /opt/airflow/dbt_project "
            "--exclude mart_demand_forecast"
        ),
        execution_timeout=timedelta(minutes=8),
    )

    t1_check >> t2_sync >> t3_check_changes >> t4_dbt
