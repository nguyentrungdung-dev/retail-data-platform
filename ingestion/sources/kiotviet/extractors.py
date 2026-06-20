"""
Extract raw data từ KiotViet API → list of dicts.

Mỗi function:
  - Nhận client + params filter (lastModifiedFrom, branchIds...)
  - Trả về list[dict] raw từ API (chưa transform)
  - KHÔNG biết gì về schema raw_* — đó là job của transformers.py

Thiết kế: pure function, không log progress chi tiết (để runner.py log).
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

from loguru import logger

from ingestion.sources.kiotviet.client import KiotVietClient

# ── Helpers ──────────────────────────────────────────────────────────

def _format_datetime(dt: datetime) -> str:
    """KiotViet expect format: 'YYYY-MM-DD HH:mm:ss' (UTC)."""
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def _collect_pages(
    client: KiotVietClient,
    path: str,
    params: dict[str, Any],
) -> list[dict[str, Any]]:
    """Collect tất cả rows từ paginated endpoint thành 1 list."""
    rows: list[dict[str, Any]] = []
    for page in client.paginated_get(path, params=params):
        rows.extend(page.get("data", []) or [])
    return rows


# ── Extract functions ───────────────────────────────────────────────

def extract_invoices(
    client: KiotVietClient,
    last_modified_from: datetime,
    branch_ids: list[int] | None = None,
    include_invoice_details: bool = True,
) -> list[dict[str, Any]]:
    """
    Lấy hóa đơn (đơn hàng đã thanh toán).

    KiotViet endpoint: /invoices
    Mỗi invoice có nested `invoiceDetails` (line items).

    Args:
        client: KiotVietClient instance
        last_modified_from: chỉ lấy invoice modified từ thời điểm này
        branch_ids: filter theo branch (None = tất cả)
        include_invoice_details: cần line items (mặc định True)
    """
    params: dict[str, Any] = {
        "lastModifiedFrom":     _format_datetime(last_modified_from),
        "includeInvoiceDelivery": "false",
        "includePayment":        "true",
    }
    if include_invoice_details:
        params["includeInvoiceDetails"] = "true"
    if branch_ids:
        params["branchIds"] = ",".join(str(b) for b in branch_ids)

    rows = _collect_pages(client, "/invoices", params)
    logger.info(f"[extract] invoices: {len(rows)} rows")
    return rows


def extract_orders(
    client: KiotVietClient,
    last_modified_from: datetime,
    branch_ids: list[int] | None = None,
) -> list[dict[str, Any]]:
    """
    Lấy đơn đặt hàng (chưa thanh toán → khác invoices).

    KiotViet endpoint: /orders
    Khác invoices ở chỗ: order là phiếu đặt, có thể chưa thanh toán hoặc đang xử lý.
    """
    params: dict[str, Any] = {
        "lastModifiedFrom":   _format_datetime(last_modified_from),
        "includeOrderDelivery": "false",
        "includePayment":      "true",
    }
    if branch_ids:
        params["branchIds"] = ",".join(str(b) for b in branch_ids)

    rows = _collect_pages(client, "/orders", params)
    logger.info(f"[extract] orders: {len(rows)} rows")
    return rows


def extract_products(
    client: KiotVietClient,
    last_modified_from: datetime,
    include_inventory: bool = True,
) -> list[dict[str, Any]]:
    """
    Lấy danh mục sản phẩm.

    KiotViet endpoint: /products
    Có sẵn nested `inventories` per branch nếu include_inventory=True.
    """
    params: dict[str, Any] = {
        "lastModifiedFrom":      _format_datetime(last_modified_from),
        "includeInventory":      str(include_inventory).lower(),
        "includePricebook":      "false",
        "includeQuantity":       str(include_inventory).lower(),
    }

    rows = _collect_pages(client, "/products", params)
    logger.info(f"[extract] products: {len(rows)} rows")
    return rows


def extract_customers(
    client: KiotVietClient,
    last_modified_from: datetime,
) -> list[dict[str, Any]]:
    """Lấy danh sách khách hàng. KiotViet endpoint: /customers"""
    params: dict[str, Any] = {
        "lastModifiedFrom": _format_datetime(last_modified_from),
    }

    rows = _collect_pages(client, "/customers", params)
    logger.info(f"[extract] customers: {len(rows)} rows")
    return rows


def extract_inventory_snapshot(
    client: KiotVietClient,
    branch_ids: list[int] | None = None,
) -> list[dict[str, Any]]:
    """
    Lấy SNAPSHOT tồn kho hiện tại — KHÁC các entity trên (không có lastModifiedFrom).

    Strategy: gọi /products?includeInventory=true rồi flatten trường `inventories`
    của từng product. Mỗi (product, branch) → 1 row inventory.
    KiotViet không có endpoint /inventories trực tiếp.
    """
    params: dict[str, Any] = {
        "includeInventory":   "true",
        "includeQuantity":    "true",
    }

    products = _collect_pages(client, "/products", params)

    # Flatten: 1 product có nhiều inventories → mỗi inventory = 1 row
    inventory_rows: list[dict[str, Any]] = []
    for product in products:
        inventories = product.get("inventories") or []
        for inv in inventories:
            if branch_ids and inv.get("branchId") not in branch_ids:
                continue
            inventory_rows.append({
                "productCode":   product.get("code"),
                "productId":     product.get("id"),
                "productName":   product.get("name"),
                "branchId":      inv.get("branchId"),
                "branchName":    inv.get("branchName"),
                "onHand":        inv.get("onHand", 0),
                "reserved":      inv.get("reserved", 0),
                "actualReserved": inv.get("actualReserved", 0),
            })

    logger.info(
        f"[extract] inventory: {len(inventory_rows)} rows "
        f"từ {len(products)} products"
    )
    return inventory_rows
