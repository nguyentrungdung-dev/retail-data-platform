"""
Cấu hình chung cho ingestion layer.
Load biến môi trường từ .env
"""
import os
from dotenv import load_dotenv
from loguru import logger

# Load .env
load_dotenv()

# Database
DB_CONFIG = {
    "user":     os.getenv("POSTGRES_USER", "retail"),
    "password": os.getenv("POSTGRES_PASSWORD", "retail123"),
    "host":     os.getenv("POSTGRES_HOST", "localhost"),
    "port":     os.getenv("POSTGRES_PORT", "5433"),
    "database": os.getenv("POSTGRES_DB", "retail_dw"),
}

DB_URL = (
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
