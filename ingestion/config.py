"""
Cấu hình chung cho ingestion layer.
Load biến môi trường từ .env nhưng KHÔNG ghi đè biến đã set bởi container.
"""
import os
from dotenv import load_dotenv
from loguru import logger

# override=False: nếu container đã set env (qua docker-compose) thì giữ nguyên,
# .env chỉ điền vào những biến CHƯA có. Đảm bảo `RETAIL_DB_CONN` từ compose
# không bị `.env` (dev local: localhost:5434) đè lên.
load_dotenv(override=False)

# Database
DB_CONFIG = {
    "user":     os.getenv("POSTGRES_USER", "retail"),
    "password": os.getenv("POSTGRES_PASSWORD", "retail123"),
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     os.getenv("POSTGRES_PORT", "5432"),
    "database": os.getenv("POSTGRES_DB", "retail_dw"),
}

# Ưu tiên RETAIL_DB_CONN (compose set sẵn cho Airflow container, trỏ tới
# postgres-dwh:5432 qua docker network). Khi chạy từ host MacBook, biến này
# không có → fallback tự ráp từ POSTGRES_HOST/PORT trong .env (localhost:5434).
DB_URL = os.getenv("RETAIL_DB_CONN") or (
    f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)

# Logging
logger.add(
    "logs/ingestion_{time:YYYY-MM-DD}.log",
    rotation="1 day",
    retention="30 days",
    level="INFO",
)
