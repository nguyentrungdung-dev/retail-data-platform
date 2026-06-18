"""
Notifications cho pipeline — multi-channel (Email + Zalo).

Mỗi channel độc lập, fail-safe: nếu chưa cấu hình env tương ứng thì skip,
KHÔNG raise lỗi để tránh che mất lỗi gốc của task.
"""
from __future__ import annotations

import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any

import requests
from dotenv import load_dotenv
from loguru import logger

load_dotenv(override=False)

# ── ENV CONFIG ──────────────────────────────────────────────────
ZALO_WEBHOOK_URL = os.getenv("ZALO_WEBHOOK_URL", "")

SMTP_HOST     = os.getenv("SMTP_HOST", "")
SMTP_PORT     = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER     = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_FROM     = os.getenv("SMTP_FROM", SMTP_USER)
SMTP_TO       = os.getenv("SMTP_TO", "")  # csv: "a@x.com,b@y.com"


# ── HELPERS ─────────────────────────────────────────────────────

def _format_duration(seconds: float | int | None) -> str:
    """Định dạng thời gian human-readable."""
    if seconds is None:
        return "n/a"
    if seconds < 60:
        return f"{seconds:.0f} giây"
    if seconds < 3600:
        return f"{seconds / 60:.1f} phút"
    return f"{seconds / 3600:.1f} giờ"


def _build_html(
    title: str,
    color: str,
    rows: list[tuple[str, str]],
    extra_html: str = "",
) -> str:
    """Build email body HTML đơn giản, tương thích mọi mail client."""
    rows_html = "".join(
        f'<tr>'
        f'<td style="padding:8px 12px;background:#f5f5f5;'
        f'border-bottom:1px solid #e0e0e0;width:140px;font-weight:600">{k}</td>'
        f'<td style="padding:8px 12px;border-bottom:1px solid #e0e0e0">{v}</td>'
        f'</tr>'
        for k, v in rows
    )
    return f"""\
<html><body style="font-family:Arial,sans-serif;max-width:640px;margin:0 auto">
  <div style="background:{color};color:white;padding:16px 20px;
              border-radius:8px 8px 0 0">
    <h2 style="margin:0;font-size:20px">{title}</h2>
  </div>
  <table style="width:100%;border-collapse:collapse;
                border:1px solid #e0e0e0;border-top:none">
    {rows_html}
  </table>
  {extra_html}
  <p style="color:#888;font-size:12px;margin-top:16px">
    Sent by retail-data-platform · Airflow
  </p>
</body></html>"""


# ── CHANNELS ────────────────────────────────────────────────────

def send_zalo_message(message: str) -> bool:
    """Gửi message dạng plaintext đến Zalo OA webhook."""
    if not ZALO_WEBHOOK_URL:
        logger.debug("ZALO_WEBHOOK_URL chưa cấu hình, bỏ qua Zalo")
        return False
    try:
        response = requests.post(
            ZALO_WEBHOOK_URL, json={"text": message}, timeout=10
        )
        ok = response.status_code == 200
        if not ok:
            logger.warning(f"Zalo trả về {response.status_code}: {response.text[:200]}")
        return ok
    except Exception as e:
        logger.error(f"Gửi Zalo thất bại: {e}")
        return False


def send_email(subject: str, html_body: str, text_body: str | None = None) -> bool:
    """
    Gửi email qua SMTP. Hỗ trợ:
      - SSL trực tiếp (port 465) hoặc STARTTLS (port 587)
      - Multi-recipient qua SMTP_TO csv
      - HTML body + plaintext fallback

    Returns True nếu đã gửi tới ≥1 recipient, False nếu chưa cấu hình hoặc lỗi.
    """
    if not SMTP_HOST or not SMTP_TO:
        logger.debug("SMTP_HOST/SMTP_TO chưa cấu hình, bỏ qua email")
        return False

    recipients = [r.strip() for r in SMTP_TO.split(",") if r.strip()]
    if not recipients:
        logger.warning("SMTP_TO rỗng sau khi parse, bỏ qua email")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = SMTP_FROM or SMTP_USER or "noreply@retail.local"
    msg["To"]      = ", ".join(recipients)

    if text_body:
        msg.attach(MIMEText(text_body, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    try:
        if SMTP_PORT == 465:
            client = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=15)
        else:
            client = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15)
            client.ehlo()
            client.starttls()
            client.ehlo()

        with client:
            if SMTP_USER:
                client.login(SMTP_USER, SMTP_PASSWORD)
            client.send_message(msg)

        logger.info(f"Email đã gửi tới {len(recipients)} recipient(s): {subject}")
        return True
    except Exception as e:
        logger.error(f"Gửi email thất bại: {e}")
        return False


# ── HIGH-LEVEL CALLBACKS ────────────────────────────────────────

def notify_success(context: dict[str, Any]) -> None:
    """
    Callback hoặc PythonOperator target khi pipeline thành công.
    Gửi qua TẤT CẢ channels đã cấu hình. Không raise nếu một channel fail.
    """
    dag_id    = context["dag"].dag_id
    exec_date = context["ds"]
    duration  = None
    dag_run   = context.get("dag_run")
    if dag_run is not None:
        try:
            duration = dag_run.get_duration()
        except Exception:
            duration = None

    # Pull stats từ task extract_and_load (nếu có)
    ti = context.get("task_instance") or context.get("ti")
    stats = {}
    if ti is not None:
        stats = ti.xcom_pull(key="etl_stats", task_ids="extract_and_load") or {}

    # Plaintext cho Zalo
    text_msg = (
        f"✅ Pipeline {dag_id} hoàn thành\n"
        f"Ngày: {exec_date}\n"
        f"Thời gian: {_format_duration(duration)}\n"
        f"📥 Inserted: {stats.get('inserted', 0)}\n"
        f"🔄 Updated:  {stats.get('updated', 0)}\n"
        f"⏭ Skipped:  {stats.get('skipped', 0)}"
    )

    # HTML cho Email
    html_msg = _build_html(
        title="✅ Pipeline thành công",
        color="#2e7d32",
        rows=[
            ("DAG",         dag_id),
            ("Ngày",        exec_date),
            ("Thời gian",   _format_duration(duration)),
            ("Inserted",    str(stats.get("inserted", 0))),
            ("Updated",     str(stats.get("updated", 0))),
            ("Skipped",     str(stats.get("skipped", 0))),
        ],
    )
    subject = f"[{dag_id}] ✅ Pipeline thành công - {exec_date}"

    logger.info(text_msg)
    send_zalo_message(text_msg)
    send_email(subject, html_body=html_msg, text_body=text_msg)


def notify_failure(context: dict[str, Any]) -> None:
    """
    Callback khi pipeline lỗi (set qua DAG-level on_failure_callback).
    """
    dag_id    = context["dag"].dag_id
    ti        = context.get("task_instance")
    task_id   = ti.task_id if ti is not None else "unknown"
    exec_date = context["ds"]
    error     = str(context.get("exception", "Unknown"))[:500]
    log_url   = ti.log_url if ti is not None and hasattr(ti, "log_url") else ""

    text_msg = (
        f"❌ Pipeline {dag_id} LỖI\n"
        f"Task: {task_id}\n"
        f"Ngày: {exec_date}\n"
        f"Lỗi: {error[:300]}"
    )

    extra_html = ""
    if log_url:
        extra_html = (
            f'<p style="margin-top:16px">'
            f'<a href="{log_url}" '
            f'style="background:#1976d2;color:white;padding:10px 18px;'
            f'border-radius:4px;text-decoration:none;display:inline-block">'
            f'Xem log trên Airflow</a></p>'
        )

    html_msg = _build_html(
        title="❌ Pipeline lỗi",
        color="#c62828",
        rows=[
            ("DAG",   dag_id),
            ("Task",  task_id),
            ("Ngày",  exec_date),
            ("Lỗi",   f'<pre style="margin:0;white-space:pre-wrap;'
                      f'font-size:12px">{error}</pre>'),
        ],
        extra_html=extra_html,
    )
    subject = f"[{dag_id}] ❌ Task {task_id} lỗi - {exec_date}"

    logger.error(text_msg)
    send_zalo_message(text_msg)
    send_email(subject, html_body=html_msg, text_body=text_msg)
