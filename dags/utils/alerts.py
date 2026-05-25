"""
Gửi thông báo khi pipeline hoàn thành hoặc lỗi.
"""
import os
import requests
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

ZALO_WEBHOOK_URL = os.getenv("ZALO_WEBHOOK_URL", "")


def send_zalo_message(message: str) -> bool:
    """Gửi message đến Zalo OA webhook."""
    if not ZALO_WEBHOOK_URL:
        logger.warning("Chưa cấu hình ZALO_WEBHOOK_URL, bỏ qua notification")
        return False

    try:
        response = requests.post(
            ZALO_WEBHOOK_URL,
            json={"text": message},
            timeout=10
        )
        return response.status_code == 200
    except Exception as e:
        logger.error(f"Gửi Zalo thất bại: {e}")
        return False


def notify_success(context):
    """Callback khi DAG chạy thành công."""
    dag_id    = context["dag"].dag_id
    exec_date = context["ds"]
    duration  = context.get("dag_run").get_duration()

    message = (
        f"✅ Pipeline hoàn thành!\n"
        f"DAG: {dag_id}\n"
        f"Ngày: {exec_date}\n"
        f"Thời gian: {duration:.0f} giây"
    )
    logger.info(message)
    send_zalo_message(message)


def notify_failure(context):
    """Callback khi DAG bị lỗi."""
    dag_id    = context["dag"].dag_id
    task_id   = context["task_instance"].task_id
    exec_date = context["ds"]
    error     = context.get("exception", "Unknown error")

    message = (
        f"❌ Pipeline lỗi!\n"
        f"DAG: {dag_id}\n"
        f"Task: {task_id}\n"
        f"Ngày: {exec_date}\n"
        f"Lỗi: {str(error)[:200]}"
    )
    logger.error(message)
    send_zalo_message(message)