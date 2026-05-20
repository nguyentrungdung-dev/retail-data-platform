"""
Load DataFrame vào PostgreSQL với upsert (insert hoặc update).
"""
import hashlib
import pandas as pd
from sqlalchemy import create_engine, text
from loguru import logger
from ingestion.config import DB_URL


def _compute_row_hash(row: pd.Series) -> str:
    """Tạo hash MD5 cho một row để detect thay đổi."""
    row_str = "|".join(str(v) for v in row.values)
    return hashlib.md5(row_str.encode()).hexdigest()


def upsert_dataframe(
    df: pd.DataFrame,
    table_name: str,
    primary_key: str,
) -> dict:
    """
    Upsert DataFrame vào PostgreSQL.
    - Insert nếu chưa có
    - Update nếu _row_hash thay đổi
    - Bỏ qua nếu data giống hệt

    Returns: dict với số rows inserted/updated/skipped
    """
    if df.empty:
        logger.warning(f"DataFrame rỗng, bỏ qua {table_name}")
        return {"inserted": 0, "updated": 0, "skipped": 0}

    # Thêm _row_hash để detect data thay đổi
    df = df.copy()
    df["_row_hash"] = df.apply(_compute_row_hash, axis=1)

    engine = create_engine(DB_URL)
    stats = {"inserted": 0, "updated": 0, "skipped": 0}

    with engine.begin() as conn:
        for _, row in df.iterrows():
            pk_value = row[primary_key]

            # Kiểm tra row đã tồn tại chưa
            existing = conn.execute(
                text(f"SELECT _row_hash FROM {table_name} WHERE {primary_key} = :pk"),
                {"pk": pk_value}
            ).fetchone()

            if existing is None:
                # INSERT mới
                cols = ", ".join(row.index)
                vals = ", ".join([f":{c}" for c in row.index])
                conn.execute(
                    text(f"INSERT INTO {table_name} ({cols}) VALUES ({vals})"),
                    row.to_dict()
                )
                stats["inserted"] += 1

            elif existing[0] != row["_row_hash"]:
                # UPDATE vì data thay đổi
                set_clause = ", ".join([
                    f"{c} = :{c}" for c in row.index
                    if c != primary_key
                ])
                conn.execute(
                    text(f"UPDATE {table_name} SET {set_clause} WHERE {primary_key} = :pk"),
                    {**row.to_dict(), "pk": pk_value}
                )
                stats["updated"] += 1

            else:
                stats["skipped"] += 1

    logger.info(
        f"[{table_name}] ✅ inserted={stats['inserted']}, "
        f"updated={stats['updated']}, skipped={stats['skipped']}"
    )
    return stats
