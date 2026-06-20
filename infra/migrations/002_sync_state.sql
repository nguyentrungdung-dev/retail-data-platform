-- ════════════════════════════════════════════
-- Migration 002: Tracking incremental sync từ KiotViet API (Bước 13)
--
-- Mỗi entity (invoices, products, customers, ...) lưu mốc lastModifiedFrom
-- để lần extract tiếp theo chỉ lấy data thay đổi từ mốc đó về sau.
--
-- Cách chạy:
--   docker exec -i retail_dwh psql -U retail -d retail_dw \
--     < infra/migrations/002_sync_state.sql
-- ════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS _sync_state (
    source_system   VARCHAR(50) NOT NULL,           -- 'kiotviet'
    entity_name     VARCHAR(50) NOT NULL,           -- 'invoices' / 'products' / ...
    last_synced_at  TIMESTAMP   NOT NULL,           -- mốc lastModifiedFrom
    last_run_at     TIMESTAMP   NOT NULL DEFAULT NOW(),
    last_run_status VARCHAR(20) NOT NULL DEFAULT 'SUCCESS',  -- SUCCESS/FAILED
    rows_fetched    INT         NOT NULL DEFAULT 0,
    rows_upserted   INT         NOT NULL DEFAULT 0,
    error_message   TEXT,
    PRIMARY KEY (source_system, entity_name)
);

-- Lịch sử mỗi lần sync (audit log)
-- Khác _sync_state (chỉ giữ trạng thái mới nhất), bảng này append-only.
CREATE TABLE IF NOT EXISTS _sync_history (
    sync_id         BIGSERIAL   PRIMARY KEY,
    source_system   VARCHAR(50) NOT NULL,
    entity_name     VARCHAR(50) NOT NULL,
    started_at      TIMESTAMP   NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMP,
    duration_ms     INT,
    status          VARCHAR(20) NOT NULL DEFAULT 'RUNNING',
    rows_fetched    INT         DEFAULT 0,
    rows_upserted   INT         DEFAULT 0,
    error_message   TEXT,
    sync_window_from TIMESTAMP,                     -- lastModifiedFrom dùng
    sync_window_to   TIMESTAMP                      -- mốc kết thúc cửa sổ
);

CREATE INDEX IF NOT EXISTS idx_sync_history_started ON _sync_history(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_sync_history_entity  ON _sync_history(source_system, entity_name);

SELECT 'Migration 002 applied successfully' AS status;
