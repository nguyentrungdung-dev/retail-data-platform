"""
Transform KiotViet API response (JSON) → DataFrame schema raw_* trong DWH.

Mỗi function:
  - Nhận list[dict] raw từ extractors.py
  - Trả về DataFrame đúng schema của bảng raw_* tương ứng
  - Pure function: dễ test với mock JSON, không cần DB

KiotViet → raw_* schema mapping:
  invoices      → raw_orders        (1 invoice → N rows, mỗi row = 1 line item)
  orders        → raw_orders        (cùng schema, chỉ khác source_system)
  products      → raw_products
  customers     → raw_customers
  inventory     → raw_inventory     (snapshot mỗi ngày)
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd
from loguru import logger

# ── Constants ────────────────────────────────────────────────────────

# Map KiotViet invoice.status → text dễ đọc
INVOICE_STATUS_MAP = {
    1: "Hoàn thành",
    2: "Đã hủy",
    3: "Đang xử lý",
    4: "Không giao được",
}

# Map KiotViet payment method
PAYMENT_METHOD_MAP = {
    "Cash":     "cash",
    "Card":     "card",
    "Transfer": "transfer",
    "Voucher":  "voucher",
    "Debt":     "debt",
}

# Map KiotViet customer type
CUSTOMER_TYPE_MAP = {
    0: "individual",        # Cá nhân
    1: "company",           # Doanh nghiệp
}


# ── Helpers ──────────────────────────────────────────────────────────

def _parse_dt(value: Any) -> datetime | None:
    """KiotViet trả ISO datetime; parse an toàn."""
    if not value:
        return None
    try:
        return pd.to_datetime(value, errors="coerce").to_pydatetime()
    except (ValueError, TypeError):
        return None


def _safe_str(value: Any) -> str | None:
    """Convert value → str; trả None nếu nan/None."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return str(value).strip() or None


def _classify_order_type(invoice: dict[str, Any]) -> str:
    """
    Phân loại đơn: retail (B2C) / wholesale / contractor (B2B).

    Heuristic dựa trên customer + giá trị đơn:
      - Có customer là Company → wholesale
      - Đơn > 50 triệu → contractor (thường nhà thầu lớn)
      - Còn lại → retail
    """
    customer_type = invoice.get("customerType")
    total = float(invoice.get("total") or 0)

    if customer_type == 1:
        return "wholesale"
    if total >= 50_000_000:
        return "contractor"
    return "retail"


# ── Transform functions ─────────────────────────────────────────────

def transform_invoices_to_orders(
    invoices: list[dict[str, Any]],
    source_system: str = "kiotviet",
) -> pd.DataFrame:
    """
    Mỗi invoice từ KiotViet có nested invoiceDetails (line items).
    EXPLODE: 1 invoice → N rows trong raw_orders.

    order_id format: '{invoice.code}_{line_index:03d}' để đảm bảo unique
    (vì raw_orders.order_id là PK).
    """
    rows: list[dict[str, Any]] = []

    for inv in invoices:
        invoice_code = _safe_str(inv.get("code"))
        if not invoice_code:
            continue

        # Skip invoice đã hủy (status=2)
        if inv.get("status") == 2:
            continue

        purchase_date = _parse_dt(inv.get("purchaseDate"))
        modified_date = _parse_dt(inv.get("modifiedDate"))
        customer_id   = _safe_str(inv.get("customerCode")) or _safe_str(inv.get("customerId"))
        customer_phone = _safe_str(inv.get("customerPhone"))
        staff_id      = _safe_str(inv.get("soldById"))
        order_type    = _classify_order_type(inv)

        # Payment method từ array payments (lấy method đầu tiên)
        payments = inv.get("payments") or []
        payment_method_raw = payments[0].get("method") if payments else None
        payment_method = PAYMENT_METHOD_MAP.get(payment_method_raw, "unknown")

        details = inv.get("invoiceDetails") or []
        if not details:
            logger.warning(f"Invoice {invoice_code} không có line items — skip")
            continue

        for line_index, line in enumerate(details, start=1):
            product_code = _safe_str(line.get("productCode"))
            if not product_code:
                continue

            qty = float(line.get("quantity") or 0)
            if qty <= 0:
                continue

            price = float(line.get("price") or 0)
            discount = float(line.get("discount") or 0)
            cost_price = float(line.get("costPrice") or 0)
            note = _safe_str(line.get("note"))

            rows.append({
                "order_id":         f"{invoice_code}_{line_index:03d}",
                "order_date":       purchase_date,
                "customer_id":      customer_id,
                "customer_phone":   customer_phone,
                "product_code":     product_code,
                "product_name":     _safe_str(line.get("productName")),
                "qty_sold":         qty,
                "selling_price":    price,
                "cost_price":       cost_price,
                "discount_amount":  discount,
                "payment_method":   payment_method,
                "order_type":       order_type,
                "staff_id":         staff_id,
                "notes":            note or _safe_str(inv.get("description")),
                "source_system":    source_system,
            })

    df = pd.DataFrame(rows)
    if df.empty:
        # Tạo schema rỗng để loader không fail
        return pd.DataFrame(columns=[
            "order_id", "order_date", "customer_id", "customer_phone",
            "product_code", "product_name", "qty_sold", "selling_price",
            "cost_price", "discount_amount", "payment_method", "order_type",
            "staff_id", "notes", "source_system",
        ])

    logger.info(
        f"[transform] {len(invoices)} invoices → {len(df)} order line items"
    )
    return df


def transform_orders_to_orders(orders: list[dict[str, Any]]) -> pd.DataFrame:
    """
    /orders endpoint trả về phiếu đặt hàng (chưa thanh toán).
    Schema gần giống invoices nhưng key tên khác (orderDetails thay vì invoiceDetails).

    Note: dùng cùng raw_orders nhưng source_system='kiotviet_order' để dbt phân biệt.
    """
    # Reuse logic của transform_invoices: chỉ cần đổi key
    invoices_like: list[dict[str, Any]] = []
    for order in orders:
        invoices_like.append({
            **order,
            "code":           order.get("code"),
            "purchaseDate":   order.get("purchaseDate") or order.get("createdDate"),
            "invoiceDetails": order.get("orderDetails") or [],
            "payments":       order.get("payments") or [],
        })

    return transform_invoices_to_orders(
        invoices_like,
        source_system="kiotviet_order",
    )


def transform_products(products: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Map KiotViet product → raw_products schema.

    KiotViet `categoryName` có dạng "Điện lạnh > Máy lạnh > Inverter":
      → category_l1 = "Điện lạnh", category_l2 = "Máy lạnh"
    """
    rows: list[dict[str, Any]] = []

    for p in products:
        product_code = _safe_str(p.get("code"))
        if not product_code:
            continue

        category_full = _safe_str(p.get("categoryName")) or ""
        parts = [x.strip() for x in category_full.split(">") if x.strip()]
        category_l1 = parts[0] if len(parts) >= 1 else None
        category_l2 = parts[1] if len(parts) >= 2 else None

        rows.append({
            "product_code":  product_code,
            "product_name":  _safe_str(p.get("name")),
            "category_l1":   category_l1,
            "category_l2":   category_l2,
            "brand":         _safe_str(p.get("tradeMarkName")),
            "unit":          _safe_str(p.get("unit")) or "cái",
            "cost_price":    float(p.get("basePrice") or 0),
            "list_price":    float(p.get("orderTemplate") or p.get("basePrice") or 0)
                                if p.get("basePrice") is not None
                                else None,
            "supplier_code": None,        # KiotViet không expose qua /products
            "is_active":     bool(p.get("isActive", True)),
        })

    df = pd.DataFrame(rows)
    logger.info(f"[transform] {len(products)} products → {len(df)} rows")
    return df


def transform_customers(customers: list[dict[str, Any]]) -> pd.DataFrame:
    """Map KiotViet customer → raw_customers schema."""
    rows: list[dict[str, Any]] = []

    for c in customers:
        customer_id = _safe_str(c.get("code"))
        if not customer_id:
            continue

        rows.append({
            "customer_id":     customer_id,
            "customer_name":   _safe_str(c.get("name")),
            "phone":           _safe_str(c.get("contactNumber")),
            "address":         _safe_str(c.get("address")),
            "customer_type":   CUSTOMER_TYPE_MAP.get(c.get("type"), "individual"),
            "first_order_date": _parse_dt(c.get("createdDate")),
        })

    df = pd.DataFrame(rows)
    logger.info(f"[transform] {len(customers)} customers → {len(df)} rows")
    return df


def transform_inventory(inventory_rows: list[dict[str, Any]]) -> pd.DataFrame:
    """
    Map flatten inventory rows → raw_inventory schema.
    snapshot_date = today (snapshot 1 lần / ngày).
    """
    today = datetime.utcnow().date()

    rows: list[dict[str, Any]] = []
    for inv in inventory_rows:
        product_code = _safe_str(inv.get("productCode"))
        if not product_code:
            continue

        rows.append({
            "snapshot_date":  today,
            "product_code":   product_code,
            "qty_on_hand":    float(inv.get("onHand") or 0),
            "qty_reserved":   float(inv.get("reserved") or 0),
            "warehouse_loc":  _safe_str(inv.get("branchName")) or "main",
        })

    df = pd.DataFrame(rows)
    logger.info(f"[transform] inventory: {len(df)} rows snapshot {today}")
    return df
