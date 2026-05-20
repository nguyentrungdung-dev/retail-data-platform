"""
Extract dữ liệu đơn hàng từ file Excel.
Hỗ trợ cả tiếng Việt và tiếng Anh cho tên cột.
"""
import pandas as pd
from pathlib import Path
from loguru import logger

# Map tên cột tiếng Việt → schema raw_orders
COLUMN_MAP = {
    # Tiếng Việt
    "mã đơn":       "order_id",
    "ngày bán":     "order_date",
    "ngày":         "order_date",
    "mã khách":     "customer_id",
    "sdt":          "customer_phone",
    "số điện thoại":"customer_phone",
    "mã sp":        "product_code",
    "mã hàng":      "product_code",
    "tên sp":       "product_name",
    "tên hàng":     "product_name",
    "số lượng":     "qty_sold",
    "sl":           "qty_sold",
    "đơn giá":      "selling_price",
    "giá bán":      "selling_price",
    "giá vốn":      "cost_price",
    "chiết khấu":   "discount_amount",
    "ck":           "discount_amount",
    "thanh toán":   "payment_method",
    "loại đơn":     "order_type",
    "loại":         "order_type",
    "ghi chú":      "notes",
    "nhân viên":    "staff_id",
    "nv":           "staff_id",
    # Tiếng Anh
    "order_id":     "order_id",
    "order_date":   "order_date",
    "date":         "order_date",
    "customer_id":  "customer_id",
    "phone":        "customer_phone",
    "product_code": "product_code",
    "product_name": "product_name",
    "qty":          "qty_sold",
    "quantity":     "qty_sold",
    "price":        "selling_price",
    "cost":         "cost_price",
    "discount":     "discount_amount",
    "payment":      "payment_method",
    "order_type":   "order_type",
    "notes":        "notes",
    "staff_id":     "staff_id",
}

REQUIRED_COLUMNS = ["order_id", "order_date", "product_code", "qty_sold", "selling_price"]


def extract_orders_from_excel(file_path: str) -> pd.DataFrame:
    """
    Đọc file Excel và trả về DataFrame theo schema raw_orders.

    Args:
        file_path: đường dẫn đến file .xlsx hoặc .xls

    Returns:
        DataFrame đã chuẩn hóa
    """
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {file_path}")

    logger.info(f"Đọc file: {path.name}")

    # Đọc tất cả sheets, tìm sheet có dữ liệu đơn hàng
    xl = pd.ExcelFile(file_path)
    target_sheet = None

    for sheet in xl.sheet_names:
        keywords = ["order", "đơn", "bán", "sale", "hóa đơn"]
        if any(k in sheet.lower() for k in keywords):
            target_sheet = sheet
            break

    # Nếu không tìm được → dùng sheet đầu tiên
    if target_sheet is None:
        target_sheet = xl.sheet_names[0]
        logger.warning(f"Không tìm thấy sheet đơn hàng, dùng sheet: '{target_sheet}'")
    else:
        logger.info(f"Dùng sheet: '{target_sheet}'")

    df = pd.read_excel(file_path, sheet_name=target_sheet)

    # Chuẩn hóa tên cột
    df.columns = [str(c).strip().lower() for c in df.columns]
    df = df.rename(columns=COLUMN_MAP)

    # Kiểm tra cột bắt buộc
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(
            f"File thiếu các cột: {missing}\n"
            f"Cột hiện có: {list(df.columns)}"
        )

    # Clean data
    df["order_date"] = pd.to_datetime(df["order_date"], dayfirst=True, errors="coerce")
    df["qty_sold"]   = pd.to_numeric(df["qty_sold"], errors="coerce")
    df["selling_price"] = pd.to_numeric(df["selling_price"], errors="coerce")
    df["cost_price"]    = pd.to_numeric(df.get("cost_price", 0), errors="coerce").fillna(0)
    df["discount_amount"] = pd.to_numeric(df.get("discount_amount", 0), errors="coerce").fillna(0)

    # Loại bỏ rows không hợp lệ
    before = len(df)
    df = df.dropna(subset=["order_id", "order_date", "qty_sold", "selling_price"])
    df = df[df["qty_sold"] > 0]
    df = df[df["selling_price"] > 0]
    after = len(df)

    if before != after:
        logger.warning(f"Loại bỏ {before - after} rows không hợp lệ")

    # Thêm metadata
    df["source_system"] = "excel"
    df["order_type"]    = df.get("order_type", "retail")

    # Chỉ giữ columns thuộc schema raw_orders
    schema_cols = [
        "order_id", "order_date", "customer_id", "customer_phone",
        "product_code", "product_name", "qty_sold", "selling_price",
        "cost_price", "discount_amount", "payment_method", "order_type",
        "staff_id", "notes", "source_system"
    ]
    df = df[[c for c in schema_cols if c in df.columns]]

    logger.info(f"Extract thành công: {len(df)} rows từ '{path.name}'")
    return df
