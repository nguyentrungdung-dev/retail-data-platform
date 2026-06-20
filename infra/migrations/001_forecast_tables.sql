-- ════════════════════════════════════════════
-- Migration 001: Tạo bảng cho ML Forecast layer (Step 12)
--
-- LƯU Ý: file này KHÁC init_db.sql. init_db.sql chỉ chạy duy nhất 1 lần
-- khi Postgres khởi tạo lần đầu. Khi đã có volume dwh-data từ trước, init
-- sẽ bị bỏ qua → cần migration thủ công như file này.
--
-- Cách chạy:
--   docker exec -i retail_dwh psql -U retail -d retail_dw \
--     < infra/migrations/001_forecast_tables.sql
--
-- Idempotent: chạy lại nhiều lần không lỗi (CREATE ... IF NOT EXISTS).
-- ════════════════════════════════════════════

-- ── Metadata mỗi lần chạy forecast ──
CREATE TABLE IF NOT EXISTS raw_forecast_runs (
    run_id            VARCHAR(64) PRIMARY KEY,
    run_started_at    TIMESTAMP   NOT NULL,
    run_finished_at   TIMESTAMP,
    horizon_days      INT         NOT NULL,
    train_window_days INT,
    sku_count         INT,
    sku_skipped       INT         DEFAULT 0,
    status            VARCHAR(20) DEFAULT 'RUNNING',
    error_message     TEXT,
    avg_mape          NUMERIC(6,2),
    notes             TEXT
);

-- ── Kết quả forecast theo từng (run_id, product_code, ds) ──
CREATE TABLE IF NOT EXISTS raw_forecast_results (
    run_id          VARCHAR(64) NOT NULL,
    product_code    VARCHAR(50) NOT NULL,
    ds              DATE        NOT NULL,
    yhat            NUMERIC(12,2),
    yhat_lower      NUMERIC(12,2),
    yhat_upper      NUMERIC(12,2),
    is_actual       BOOLEAN     DEFAULT FALSE,
    actual_qty      NUMERIC(12,2),
    model_type      VARCHAR(20) DEFAULT 'prophet',
    created_at      TIMESTAMP   DEFAULT NOW(),
    PRIMARY KEY (run_id, product_code, ds),
    FOREIGN KEY (run_id) REFERENCES raw_forecast_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_forecast_results_product ON raw_forecast_results(product_code);
CREATE INDEX IF NOT EXISTS idx_forecast_results_ds      ON raw_forecast_results(ds);
CREATE INDEX IF NOT EXISTS idx_forecast_runs_started    ON raw_forecast_runs(run_started_at);

-- Verify
SELECT
    table_name,
    (SELECT count(*) FROM information_schema.columns
     WHERE table_name = t.table_name) AS column_count
FROM information_schema.tables t
WHERE table_schema = 'public'
  AND table_name IN ('raw_forecast_runs', 'raw_forecast_results')
ORDER BY table_name;
