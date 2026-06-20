"""
Unit test cho ingestion/sources/kiotviet/transformers.py

Pure function tests — không cần DB, không cần API thật.
Chạy: pytest tests/test_kiotviet_transformers.py -v
"""
from __future__ import annotations

import pandas as pd
import pytest

from ingestion.sources.kiotviet.transformers import (
    transform_customers,
    transform_inventory,
    transform_invoices_to_orders,
    transform_orders_to_orders,
    transform_products,
)


# ── Fixtures ────────────────────────────────────────────────────────

@pytest.fixture
def sample_invoice() -> dict:
    """Mock invoice từ KiotViet với 2 line items."""
    return {
        "id":              12345,
        "code":            "HD000001",
        "purchaseDate":    "2026-06-15T10:30:00",
        "modifiedDate":    "2026-06-15T10:35:00",
        "status":          1,                    # Hoàn thành
        "customerId":      100,
        "customerCode":    "KH001",
        "customerName":    "Nguyễn Văn A",
        "customerPhone":   "0901234567",
        "soldById":        5,
        "total":           5500000,
        "totalPayment":    5500000,
        "description":     "Đơn của khách quen",
        "invoiceDetails": [
            {
                "productId":   1001,
                "productCode": "AC001",
                "productName": "Máy lạnh ABC",
                "quantity":    1,
                "price":       5000000,
                "discount":    500000,
                "costPrice":   3500000,
            },
            {
                "productId":   1002,
                "productCode": "FAN001",
                "productName": "Quạt XYZ",
                "quantity":    2,
                "price":       500000,
                "discount":    0,
                "costPrice":   300000,
            },
        ],
        "payments": [
            {"method": "Cash", "amount": 5500000}
        ],
    }


@pytest.fixture
def sample_product() -> dict:
    return {
        "id":             5001,
        "code":           "AC001",
        "name":           "Máy lạnh ABC 1HP",
        "categoryName":   "Điện lạnh > Máy lạnh > Inverter",
        "tradeMarkName":  "Daikin",
        "unit":           "cái",
        "basePrice":      3500000,
        "isActive":       True,
    }


@pytest.fixture
def sample_customer() -> dict:
    return {
        "id":            100,
        "code":          "KH001",
        "name":          "Nguyễn Văn A",
        "contactNumber": "0901234567",
        "address":       "123 Lê Lợi, Q1, TPHCM",
        "type":          0,
        "createdDate":   "2025-01-15T08:00:00",
    }


# ── Tests: transform_invoices_to_orders ─────────────────────────────

class TestTransformInvoices:

    def test_explode_line_items(self, sample_invoice: dict):
        """1 invoice 2 lines → 2 rows trong raw_orders."""
        df = transform_invoices_to_orders([sample_invoice])

        assert len(df) == 2
        assert df.iloc[0]["order_id"] == "HD000001_001"
        assert df.iloc[1]["order_id"] == "HD000001_002"

    def test_order_id_unique_per_line(self, sample_invoice: dict):
        df = transform_invoices_to_orders([sample_invoice])
        assert df["order_id"].is_unique

    def test_skip_canceled_invoice(self, sample_invoice: dict):
        """Invoice status=2 (đã hủy) → skip hoàn toàn."""
        sample_invoice["status"] = 2
        df = transform_invoices_to_orders([sample_invoice])
        assert df.empty

    def test_skip_invoice_no_line_items(self, sample_invoice: dict):
        sample_invoice["invoiceDetails"] = []
        df = transform_invoices_to_orders([sample_invoice])
        assert df.empty

    def test_skip_line_item_zero_quantity(self, sample_invoice: dict):
        sample_invoice["invoiceDetails"][0]["quantity"] = 0
        df = transform_invoices_to_orders([sample_invoice])
        assert len(df) == 1                       # Chỉ còn 1 line
        assert df.iloc[0]["product_code"] == "FAN001"

    def test_payment_method_mapping(self, sample_invoice: dict):
        df = transform_invoices_to_orders([sample_invoice])
        assert df.iloc[0]["payment_method"] == "cash"

    def test_classify_b2b_for_high_value(self, sample_invoice: dict):
        """Đơn > 50tr → contractor."""
        sample_invoice["total"] = 60_000_000
        df = transform_invoices_to_orders([sample_invoice])
        assert df.iloc[0]["order_type"] == "contractor"

    def test_classify_b2b_for_company_customer(self, sample_invoice: dict):
        sample_invoice["customerType"] = 1        # Company
        df = transform_invoices_to_orders([sample_invoice])
        assert df.iloc[0]["order_type"] == "wholesale"

    def test_classify_b2c_for_normal_retail(self, sample_invoice: dict):
        df = transform_invoices_to_orders([sample_invoice])
        assert df.iloc[0]["order_type"] == "retail"

    def test_source_system_default_kiotviet(self, sample_invoice: dict):
        df = transform_invoices_to_orders([sample_invoice])
        assert (df["source_system"] == "kiotviet").all()

    def test_empty_input_returns_empty_df_with_schema(self):
        df = transform_invoices_to_orders([])
        assert df.empty
        assert "order_id" in df.columns
        assert "qty_sold" in df.columns

    def test_revenue_calculation(self, sample_invoice: dict):
        """Verify mapping qty + price + discount đúng."""
        df = transform_invoices_to_orders([sample_invoice])

        ac001 = df[df["product_code"] == "AC001"].iloc[0]
        assert float(ac001["qty_sold"]) == 1
        assert float(ac001["selling_price"]) == 5000000
        assert float(ac001["discount_amount"]) == 500000
        assert float(ac001["cost_price"]) == 3500000


# ── Tests: transform_orders_to_orders ───────────────────────────────

class TestTransformOrders:

    def test_uses_orderDetails_field(self, sample_invoice: dict):
        """Orders endpoint dùng `orderDetails` thay vì `invoiceDetails`."""
        order = {
            "code":          "DH000001",
            "purchaseDate":  "2026-06-15T10:30:00",
            "status":        1,
            "orderDetails":  sample_invoice["invoiceDetails"],
            "payments":      sample_invoice["payments"],
        }
        df = transform_orders_to_orders([order])
        assert len(df) == 2
        assert df.iloc[0]["source_system"] == "kiotviet_order"


# ── Tests: transform_products ───────────────────────────────────────

class TestTransformProducts:

    def test_basic_mapping(self, sample_product: dict):
        df = transform_products([sample_product])
        assert len(df) == 1

        row = df.iloc[0]
        assert row["product_code"] == "AC001"
        assert row["product_name"] == "Máy lạnh ABC 1HP"
        assert row["brand"] == "Daikin"

    def test_category_split_by_arrow(self, sample_product: dict):
        df = transform_products([sample_product])
        row = df.iloc[0]
        assert row["category_l1"] == "Điện lạnh"
        assert row["category_l2"] == "Máy lạnh"

    def test_category_only_l1(self, sample_product: dict):
        sample_product["categoryName"] = "Điện lạnh"
        df = transform_products([sample_product])
        row = df.iloc[0]
        assert row["category_l1"] == "Điện lạnh"
        assert row["category_l2"] is None

    def test_skip_product_no_code(self, sample_product: dict):
        sample_product["code"] = None
        df = transform_products([sample_product])
        assert df.empty

    def test_default_unit(self, sample_product: dict):
        sample_product["unit"] = None
        df = transform_products([sample_product])
        assert df.iloc[0]["unit"] == "cái"


# ── Tests: transform_customers ──────────────────────────────────────

class TestTransformCustomers:

    def test_basic_mapping(self, sample_customer: dict):
        df = transform_customers([sample_customer])
        assert len(df) == 1

        row = df.iloc[0]
        assert row["customer_id"] == "KH001"
        assert row["phone"] == "0901234567"
        assert row["customer_type"] == "individual"

    def test_company_customer(self, sample_customer: dict):
        sample_customer["type"] = 1
        df = transform_customers([sample_customer])
        assert df.iloc[0]["customer_type"] == "company"


# ── Tests: transform_inventory ──────────────────────────────────────

class TestTransformInventory:

    def test_basic_mapping(self):
        rows = [{
            "productCode": "AC001",
            "productId":   1001,
            "branchId":    1,
            "branchName":  "Chi nhánh chính",
            "onHand":      50,
            "reserved":    5,
        }]
        df = transform_inventory(rows)
        assert len(df) == 1

        row = df.iloc[0]
        assert row["product_code"] == "AC001"
        assert float(row["qty_on_hand"]) == 50
        assert float(row["qty_reserved"]) == 5
        assert row["warehouse_loc"] == "Chi nhánh chính"

    def test_snapshot_date_is_today(self):
        from datetime import datetime
        rows = [{"productCode": "AC001", "onHand": 10, "branchName": "main"}]
        df = transform_inventory(rows)
        assert df.iloc[0]["snapshot_date"] == datetime.utcnow().date()

    def test_skip_row_no_product_code(self):
        rows = [{"productCode": None, "onHand": 10}]
        df = transform_inventory(rows)
        assert df.empty


# ── Edge cases ───────────────────────────────────────────────────────

class TestEdgeCases:

    def test_empty_inputs_for_all_transformers(self):
        """Tất cả transformer trả empty DataFrame khi input rỗng."""
        assert transform_invoices_to_orders([]).empty
        assert transform_orders_to_orders([]).empty
        assert transform_products([]).empty
        assert transform_customers([]).empty
        assert transform_inventory([]).empty

    def test_invoice_with_null_payment(self, sample_invoice: dict):
        sample_invoice["payments"] = []
        df = transform_invoices_to_orders([sample_invoice])
        assert df.iloc[0]["payment_method"] == "unknown"
