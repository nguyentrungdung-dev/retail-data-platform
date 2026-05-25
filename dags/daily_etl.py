"""
Retail Data Platform — Main ETL DAG
Chạy mỗi đêm 2:00 AM
Flow: Extract → Load → dbt build → Notify
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Default args áp dụng cho tất cả tasks
default_args = {
    "owner":            "retail",
    "depends_on_past":  False,
    "start_date":       days_ago(1),
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          3,
    "retry_delay":      timedelta(minutes=5),
}

# ── TASK FUNCTIONS ──────────────────────────────────────────────

def check_source(**context):
    """Kiểm tra file nguồn hoặc API có sẵn sàng không."""
    import os
    from loguru import logger

    # Kiểm tra thư mục data/samples có file Excel không
    data_dir = "/opt/airflow/data/samples"
    if os.path.exists(data_dir):
        files = [f for f in os.listdir(data_dir) if f.endswith(".xlsx")]
        if files:
            logger.info(f"Tìm thấy {len(files)} file Excel: {files}")
            context["ti"].xcom_push(key="excel_files", value=files)
            return True

    logger.warning("Không tìm thấy file Excel, sẽ dùng sample data")
    return True


def extract_and_load(**context):
    """Extract từ Excel và load vào PostgreSQL."""
    import os
    import sys
    sys.path.insert(0, "/opt/airflow")

    from loguru import logger
    from ingestion.sources.excel import extract_orders_from_excel
    from ingestion.loaders.postgres_loader import upsert_dataframe

    data_dir = "/opt/airflow/data/samples"
    excel_files = context["ti"].xcom_pull(
        key="excel_files",
        task_ids="check_source"
    ) or ["orders_sample.xlsx"]

    total_stats = {"inserted": 0, "updated": 0, "skipped": 0}

    for filename in excel_files:
        file_path = os.path.join(data_dir, filename)
        if not os.path.exists(file_path):
            logger.warning(f"Không tìm thấy file: {file_path}")
            continue

        logger.info(f"Xử lý file: {filename}")

        # Extract
        df = extract_orders_from_excel(file_path)
        logger.info(f"Extract: {len(df)} rows")

        # Load
        stats = upsert_dataframe(df, "raw_orders", "order_id")

        # Cộng dồn stats
        for k in total_stats:
            total_stats[k] += stats.get(k, 0)

    logger.info(f"Tổng kết ETL: {total_stats}")

    # Push stats để notify_success dùng
    context["ti"].xcom_push(key="etl_stats", value=total_stats)
    return total_stats


def send_success_notification(**context):
    """Gửi thông báo thành công kèm stats."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    from dags.utils.alerts import send_zalo_message
    from loguru import logger

    etl_stats = context["ti"].xcom_pull(
        key="etl_stats",
        task_ids="extract_and_load"
    ) or {}

    exec_date = context["ds"]
    message = (
        f"✅ Pipeline hoàn thành!\n"
        f"Ngày chạy: {exec_date}\n"
        f"📥 Inserted: {etl_stats.get('inserted', 0)}\n"
        f"🔄 Updated:  {etl_stats.get('updated', 0)}\n"
        f"⏭ Skipped:  {etl_stats.get('skipped', 0)}\n"
        f"📊 dbt models đã refresh thành công"
    )
    logger.info(message)
    send_zalo_message(message)


def send_failure_notification(**context):
    """Gửi thông báo lỗi."""
    import sys
    sys.path.insert(0, "/opt/airflow")

    from dags.utils.alerts import send_zalo_message

    task_id   = context["task_instance"].task_id
    exec_date = context["ds"]
    error     = str(context.get("exception", "Unknown"))[:300]

    message = (
        f"❌ Pipeline lỗi!\n"
        f"Task: {task_id}\n"
        f"Ngày: {exec_date}\n"
        f"Lỗi: {error}"
    )
    send_zalo_message(message)


# ── DAG DEFINITION ──────────────────────────────────────────────

with DAG(
    dag_id="retail_daily_etl",
    description="Retail Data Platform — ETL pipeline hàng ngày",
    default_args=default_args,
    schedule_interval="0 2 * * *",   # 2:00 AM mỗi ngày
    catchup=False,
    max_active_runs=1,
    tags=["retail", "etl", "daily"],
    on_failure_callback=send_failure_notification,
) as dag:

    # ── TASK 1: Kiểm tra nguồn dữ liệu
    t1_check = PythonOperator(
        task_id="check_source",
        python_callable=check_source,
    )

    # ── TASK 2: Extract + Load
    t2_etl = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load,
        execution_timeout=timedelta(minutes=30),
    )

    # ── TASK 3: dbt build (run + test)
    t3_dbt = BashOperator(
        task_id="dbt_build",
        bash_command=(
            "cd /opt/airflow/dbt_project && "
            "dbt build --profiles-dir /opt/airflow/dbt_project"
        ),
        execution_timeout=timedelta(minutes=20),
    )

    # ── TASK 4: Thông báo thành công
    t4_notify = PythonOperator(
        task_id="notify_success",
        python_callable=send_success_notification,
        trigger_rule="all_success",   # chỉ chạy nếu tất cả task trước OK
    )

    # ── DEPENDENCIES: t1 → t2 → t3 → t4
    t1_check >> t2_etl >> t3_dbt >> t4_notify