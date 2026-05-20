-- ════════════════════════════════════════════
-- Retail Data Platform — Database Schema
-- Raw layer: dữ liệu gốc từ nguồn (KiotViet, file Excel, ...)
-- ════════════════════════════════════════════

-- ── Đơn hàng (mỗi dòng = 1 line-item) ──
CREATE TABLE IF NOT EXISTS raw_orders (
    order_id        VARCHAR(50) PRIMARY KEY,
    order_date      TIMESTAMP,
    customer_id     VARCHAR(50),
    customer_phone  VARCHAR(20),
    product_code    VARCHAR(50),
    product_name    VARCHAR(255),
    qty_sold        NUMERIC(10,2),
    selling_price   NUMERIC(15,2),
    cost_price      NUMERIC(15,2),
    discount_amount NUMERIC(15,2) DEFAULT 0,
    payment_method  VARCHAR(50),
    order_type      VARCHAR(50),
    staff_id        VARCHAR(50),
    notes           TEXT,
    source_system   VARCHAR(50),
    _row_hash       VARCHAR(64),
    ingested_at     TIMESTAMP DEFAULT NOW()
);

-- ── Danh mục sản phẩm ──
CREATE TABLE IF NOT EXISTS raw_products (
    product_code    VARCHAR(50) PRIMARY KEY,
    product_name    VARCHAR(255),
    category_l1     VARCHAR(100),
    category_l2     VARCHAR(100),
    brand           VARCHAR(100),
    unit            VARCHAR(20),
    cost_price      NUMERIC(15,2),
    list_price      NUMERIC(15,2),
    supplier_code   VARCHAR(50),
    is_active       BOOLEAN DEFAULT TRUE,
    _row_hash       VARCHAR(64),
    ingested_at     TIMESTAMP DEFAULT NOW()
);

-- ── Tồn kho snapshot theo ngày ──
CREATE TABLE IF NOT EXISTS raw_inventory (
    snapshot_date   DATE,
    product_code    VARCHAR(50),
    qty_on_hand     NUMERIC(10,2),
    qty_reserved    NUMERIC(10,2) DEFAULT 0,
    warehouse_loc   VARCHAR(50) DEFAULT 'main',
    _row_hash       VARCHAR(64),
    ingested_at     TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, product_code)
);

-- ── Khách hàng ──
CREATE TABLE IF NOT EXISTS raw_customers (
    customer_id     VARCHAR(50) PRIMARY KEY,
    customer_name   VARCHAR(255),
    phone           VARCHAR(20),
    address         TEXT,
    customer_type   VARCHAR(50),
    first_order_date DATE,
    _row_hash       VARCHAR(64),
    ingested_at     TIMESTAMP DEFAULT NOW()
);

-- ── Index hỗ trợ truy vấn theo thời gian ──
CREATE INDEX IF NOT EXISTS idx_raw_orders_date     ON raw_orders(order_date);
CREATE INDEX IF NOT EXISTS idx_raw_orders_product  ON raw_orders(product_code);
CREATE INDEX IF NOT EXISTS idx_raw_inventory_date  ON raw_inventory(snapshot_date);

SELECT 'Database initialized successfully' AS status;
