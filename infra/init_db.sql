-- ════════════════════════════════════════════
-- Retail Data Platform — Database Schema
-- Raw layer: dữ liệu gốc từ nguồn (KiotViet, file Excel, ...)
-- ════════════════════════════════════════════

-- ── Tạo database phụ cho Metabase metadata ──
-- Metabase cần 1 database riêng để lưu user/dashboards/queries của nó.
-- Compose set MB_DB_DBNAME=metabase nên tên này là bắt buộc.
-- DO block để idempotent: chạy lại không lỗi.
SELECT 'CREATE DATABASE metabase'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'metabase')\gexec

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

-- ════════════════════════════════════════════
-- ML / Forecast layer (Step 12)
-- ════════════════════════════════════════════

-- ── Metadata mỗi lần chạy forecast ──
-- Mỗi run = 1 lần job Prophet chạy, ghi lại tham số + status để debug.
CREATE TABLE IF NOT EXISTS raw_forecast_runs (
    run_id          VARCHAR(64) PRIMARY KEY,           -- uuid hoặc timestamp
    run_started_at  TIMESTAMP   NOT NULL,
    run_finished_at TIMESTAMP,
    horizon_days    INT         NOT NULL,              -- số ngày forecast
    train_window_days INT,                             -- bao nhiêu ngày data để train
    sku_count       INT,                               -- số SKU đã forecast
    sku_skipped     INT         DEFAULT 0,             -- SKU bị skip do thiếu data
    status          VARCHAR(20) DEFAULT 'RUNNING',     -- RUNNING/SUCCESS/FAILED
    error_message   TEXT,
    avg_mape        NUMERIC(6,2),                      -- Mean Absolute Percentage Error trung bình
    notes           TEXT
);

-- ── Kết quả forecast theo từng (run_id, product_code, ds) ──
-- Dùng partial unique để upsert: nếu chạy lại cùng run_id → update.
CREATE TABLE IF NOT EXISTS raw_forecast_results (
    run_id          VARCHAR(64) NOT NULL,
    product_code    VARCHAR(50) NOT NULL,
    ds              DATE        NOT NULL,             -- ngày forecast
    yhat            NUMERIC(12,2),                    -- giá trị dự báo (qty_sold)
    yhat_lower      NUMERIC(12,2),                    -- cận dưới interval 80%
    yhat_upper      NUMERIC(12,2),                    -- cận trên interval 80%
    is_actual       BOOLEAN     DEFAULT FALSE,        -- TRUE = ngày trong quá khứ (để compare)
    actual_qty      NUMERIC(12,2),                    -- nếu is_actual=TRUE thì điền thực tế
    model_type      VARCHAR(20) DEFAULT 'prophet',
    created_at      TIMESTAMP   DEFAULT NOW(),
    PRIMARY KEY (run_id, product_code, ds),
    FOREIGN KEY (run_id) REFERENCES raw_forecast_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_forecast_results_product ON raw_forecast_results(product_code);
CREATE INDEX IF NOT EXISTS idx_forecast_results_ds      ON raw_forecast_results(ds);
CREATE INDEX IF NOT EXISTS idx_forecast_runs_started    ON raw_forecast_runs(run_started_at);

-- ════════════════════════════════════════════
-- Sync state layer (Step 13 — KiotViet API)
-- ════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS _sync_state (
    source_system   VARCHAR(50) NOT NULL,
    entity_name     VARCHAR(50) NOT NULL,
    last_synced_at  TIMESTAMP   NOT NULL,
    last_run_at     TIMESTAMP   NOT NULL DEFAULT NOW(),
    last_run_status VARCHAR(20) NOT NULL DEFAULT 'SUCCESS',
    rows_fetched    INT         NOT NULL DEFAULT 0,
    rows_upserted   INT         NOT NULL DEFAULT 0,
    error_message   TEXT,
    PRIMARY KEY (source_system, entity_name)
);

CREATE TABLE IF NOT EXISTS _sync_history (
    sync_id          BIGSERIAL   PRIMARY KEY,
    source_system    VARCHAR(50) NOT NULL,
    entity_name      VARCHAR(50) NOT NULL,
    started_at       TIMESTAMP   NOT NULL DEFAULT NOW(),
    finished_at      TIMESTAMP,
    duration_ms      INT,
    status           VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    rows_fetched     INT         DEFAULT 0,
    rows_upserted    INT         DEFAULT 0,
    error_message    TEXT,
    sync_window_from TIMESTAMP,
    sync_window_to   TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sync_history_started ON _sync_history(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_sync_history_entity  ON _sync_history(source_system, entity_name);

SELECT 'Database initialized successfully' AS status;
