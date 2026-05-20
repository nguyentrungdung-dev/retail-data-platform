"""
Chạy toàn bộ ETL pipeline thủ công.
Usage: python ingestion/run_etl.py --file data/samples/orders_sample.xlsx
"""
import sys
import click
from loguru import logger
from ingestion.sources.excel import extract_orders_from_excel
from ingestion.loaders.postgres_loader import upsert_dataframe


@click.command()
@click.option("--file", required=True, help="Đường dẫn file Excel")
def run(file: str):
    logger.info("=" * 50)
    logger.info("BẮT ĐẦU ETL PIPELINE")
    logger.info("=" * 50)

    # 1. Extract
    logger.info(f"[EXTRACT] Đọc file: {file}")
    df = extract_orders_from_excel(file)
    logger.info(f"[EXTRACT] Xong: {len(df)} rows")

    # 2. Load
    logger.info("[LOAD] Đưa vào PostgreSQL...")
    stats = upsert_dataframe(df, "raw_orders", "order_id")

    # 3. Summary
    logger.info("=" * 50)
    logger.info(f"HOÀN THÀNH: {stats}")
    logger.info("=" * 50)


if __name__ == "__main__":
    run()
