# 🏪 Retail Data Platform — CLAUDE.md

> **Đây là file context chính cho Claude Code.**
> Mọi session làm việc đều đọc file này đầu tiên.

---

## 📌 Project Overview

**Tên project:** `retail-data-platform`
**Mục tiêu:** Phân tích hành vi mua hàng & tồn kho thông minh cho cửa hàng điện máy, đồ gia dụng, nội thất tại Việt Nam (huyện/tỉnh lẻ, khách hàng B2C + B2B contractors).

**Ba câu hỏi business cần trả lời:**
1. Sản phẩm nào bán chạy / chậm theo mùa? Theo khu vực?
2. Khách nào có giá trị cao? Ai sắp rời đi (churn)?
3. Tồn kho nào đang thừa / thiếu so với nhu cầu thực?

---

## 🏗 Architecture

```
[Data Sources]
   KiotViet API / Excel / Google Sheets / Manual nhập
        │
        ▼
[Ingestion Layer]  ingestion/
   Python + pandas + requests
   → Chuẩn hóa → raw_* tables
        │
        ▼
[Storage Layer]    PostgreSQL (local / cloud)
   raw_*           ← dữ liệu gốc, không chỉnh sửa
   stg_*           ← đã clean, typed
   fct_* / dim_*   ← star schema
   mart_*          ← aggregated for BI
        │
        ▼
[Transform Layer]  dbt_project/
   dbt Core (SQL-first)
        │
        ▼
[Orchestration]    dags/
   Apache Airflow 2.x
   Schedule: 02:00 AM daily
        │
        ▼
[Consumption]      dashboards/
   Power BI Desktop / Metabase
```

---

## 🛠 Tech Stack

| Layer | Tool | Lý do chọn |
|---|---|---|
| Ingestion | Python 3.11, pandas, requests | Linh hoạt, đọc mọi nguồn |
| Database | PostgreSQL 15 | Mạnh, free, SQL chuẩn |
| Data Lake | MinIO (S3-compatible) | On-prem, free |
| Transform | dbt Core | SQL-first, có test, lineage |
| Orchestration | Apache Airflow 2.9 | Phổ biến, nhiều tài liệu |
| BI | Power BI + Metabase | Power BI cho report, Metabase cho self-service |
| Infra | Docker Compose | Chạy local → dễ migrate cloud |
| IaC | Terraform (phase 2) | Khi lên cloud |

---

## 📂 Project Structure

```
retail-data-platform/
├── CLAUDE.md                    ← File này (đọc trước mọi thứ)
├── .env.example                 ← Template biến môi trường
├── .env                         ← KHÔNG commit lên Git
├── .gitignore
├── docker-compose.yml           ← Toàn bộ infra local
├── Makefile                     ← Shortcuts: make up, make test, make run
│
├── ingestion/                   ← Python ETL scripts
│   ├── __init__.py
│   ├── config.py                ← Load .env, logging setup
│   ├── sources/
│   │   ├── kiotviet/            ← KiotViet API package (PRIMARY source)
│   │   │   ├── auth.py          ← OAuth2 token manager (cache 23h)
│   │   │   ├── client.py        ← HTTP client (retry, pagination, rate limit)
│   │   │   ├── extractors.py    ← 5 extract functions
│   │   │   ├── transformers.py  ← Map KiotViet schema → raw_*
│   │   │   └── runner.py        ← Orchestrate sync 5 entities
│   │   ├── excel.py             ← FALLBACK: nhập từ Excel khi API down
│   │   └── manual_entry.py     ← FALLBACK: form nhập tay
│   ├── forecasting/             ← Prophet ML (Bước 12)
│   │   ├── prophet_forecast.py
│   │   ├── seasonality.py       ← VN holidays cho Prophet
│   │   └── runner.py
│   ├── loaders/
│   │   ├── postgres_loader.py  ← Upsert vào raw_* tables
│   │   └── sync_state.py        ← Track incremental sync (Bước 13)
│   └── utils/
│       ├── logger.py
│       └── validators.py
│
├── dbt_project/                 ← dbt transformations
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/             ← stg_orders, stg_products...
│   │   ├── intermediate/        ← int_customer_metrics...
│   │   ├── marts/
│   │   │   ├── sales/           ← mart_sales_daily, mart_rfm
│   │   │   └── inventory/       ← mart_inventory_health
│   │   └── sources.yml
│   └── tests/                   ← dbt data tests
│
├── dags/                        ← Airflow DAGs
│   ├── daily_etl.py             ← Daily 02:00 — Excel + dbt build
│   ├── kiotviet_polling_etl.py  ← Mỗi 15' — sync KiotViet incremental
│   ├── weekly_forecast.py       ← Chủ Nhật 03:00 — Prophet forecast 90d
│   └── utils/
│       └── alerts.py            ← Zalo/Email notification

├── infra/
│   ├── init_db.sql              ← DDL khởi tạo (chỉ chạy lần đầu)
│   └── migrations/              ← SQL migrations cho schema thay đổi
│       ├── 001_forecast_tables.sql
│       └── 002_sync_state.sql
│
├── dashboards/
│   ├── power_bi/                ← .pbix files
│   └── metabase/                ← Dashboard JSON exports
│
├── docs/
│   ├── data_dictionary.md       ← Giải thích mọi bảng/cột
│   ├── business_rules.md        ← Quy tắc tính toán
│   └── runbook.md               ← Hướng dẫn vận hành
│
└── tests/                       ← Python unit tests
    └── test_ingestion.py
```

---

## 🗄 Data Schema

### Nguồn dữ liệu (raw layer)

```sql
-- raw_orders: Đơn hàng từ POS / KiotViet
CREATE TABLE raw_orders (
    order_id        VARCHAR PRIMARY KEY,
    order_date      TIMESTAMP,
    customer_id     VARCHAR,
    customer_phone  VARCHAR,
    product_code    VARCHAR,
    product_name    VARCHAR,
    qty_sold        NUMERIC,
    selling_price   NUMERIC,
    cost_price      NUMERIC,
    discount_amount NUMERIC DEFAULT 0,
    payment_method  VARCHAR,  -- 'cash','transfer','installment'
    order_type      VARCHAR,  -- 'retail','wholesale','contractor'
    staff_id        VARCHAR,
    notes           TEXT,
    source_system   VARCHAR,  -- 'kiotviet','excel','manual'
    ingested_at     TIMESTAMP DEFAULT NOW(),
    _row_hash       VARCHAR   -- detect changes
);

-- raw_products: Danh mục sản phẩm
CREATE TABLE raw_products (
    product_code    VARCHAR PRIMARY KEY,
    product_name    VARCHAR,
    category_l1     VARCHAR,  -- 'Điện lạnh','Điện gia dụng','Nội thất'
    category_l2     VARCHAR,  -- 'Máy lạnh','Tủ lạnh',...
    brand           VARCHAR,
    unit            VARCHAR,
    cost_price      NUMERIC,
    list_price      NUMERIC,
    supplier_code   VARCHAR,
    is_active       BOOLEAN DEFAULT TRUE,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

-- raw_inventory: Snapshot tồn kho mỗi ngày
CREATE TABLE raw_inventory (
    snapshot_date   DATE,
    product_code    VARCHAR,
    qty_on_hand     NUMERIC,
    qty_reserved    NUMERIC,
    warehouse_loc   VARCHAR DEFAULT 'main',
    ingested_at     TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (snapshot_date, product_code)
);

-- raw_customers: Thông tin khách hàng
CREATE TABLE raw_customers (
    customer_id     VARCHAR PRIMARY KEY,
    customer_name   VARCHAR,
    phone           VARCHAR,
    address         VARCHAR,
    customer_type   VARCHAR,  -- 'individual','contractor','developer'
    first_order_date DATE,
    ingested_at     TIMESTAMP DEFAULT NOW()
);
```

### Mart layer (cho BI)

```sql
-- mart_sales_daily: Doanh thu tổng hợp theo ngày
-- mart_rfm: Phân khúc khách hàng (Recency, Frequency, Monetary)
-- mart_inventory_health: Tình trạng tồn kho (hàng chậm, sắp hết)
-- mart_product_performance: Hiệu suất từng sản phẩm
```

---

## 📐 Business Rules

```python
# Tài chính
doanh_thu_thuan = qty_sold * selling_price - discount_amount
loi_nhuan_gop   = doanh_thu_thuan - (qty_sold * cost_price)
ty_le_loi_nhuan = loi_nhuan_gop / doanh_thu_thuan

# Tồn kho
hang_cham_ban   = tồn > 60 ngày không có đơn hàng
hang_het_hang   = qty_on_hand <= reorder_point (mặc định 2 units)
vong_quay_ton   = doanh_thu_thuan / ((ton_dau + ton_cuoi) / 2)

# Phân khúc khách hàng (RFM)
khach_vip       = monetary >= 20,000,000 VND / 12 tháng
khach_co_nguy_co_roi = recency > 180 ngày, từng là VIP
khach_moi       = first_order trong 30 ngày gần nhất

# Nhóm khách
B2C_le          = order_type = 'retail'
B2B_contractor  = order_type in ('wholesale', 'contractor')
```

---

## 🤖 Hướng dẫn Claude Code

### Nguyên tắc coding

1. **Python:** PEP8, type hints bắt buộc, docstring tiếng Việt OK
2. **SQL:** lowercase keywords, snake_case names, comment giải thích logic
3. **Error handling:** Mọi function phải có try/except + logging
4. **Idempotency:** Chạy lại pipeline nhiều lần phải cho cùng kết quả
5. **Secrets:** Không bao giờ hardcode credentials — dùng `.env` + `os.getenv()`

### Naming conventions

| Object | Convention | Ví dụ |
|---|---|---|
| Raw tables | `raw_<entity>` | `raw_orders` |
| Staging | `stg_<entity>` | `stg_orders` |
| Fact tables | `fct_<event>` | `fct_sales` |
| Dim tables | `dim_<entity>` | `dim_products` |
| Mart | `mart_<domain>_<topic>` | `mart_sales_daily` |
| Python files | `snake_case.py` | `kiotviet_extractor.py` |
| DAG id | `<domain>_<frequency>` | `retail_daily_etl` |

### Pattern mẫu cho từng loại task

**Khi tạo Python extractor mới:**
```
"Tạo file ingestion/sources/<source>.py để extract <data>
từ <source system>. Input: <params>. Output: DataFrame với schema <columns>.
Dùng pattern từ ingestion/sources/excel.py làm reference."
```

**Khi tạo dbt model mới:**
```
"Tạo dbt model models/<layer>/<model_name>.sql.
Input: {{ ref('<upstream_model>') }}.
Logic: <business rule>.
Output columns: <list>.
Thêm schema test: not_null cho <cols>, unique cho <col>."
```

**Khi tạo Airflow DAG:**
```
"Thêm task <task_name> vào DAG retail_daily_etl.
Task này chạy <script/function>.
Dependency: chạy sau <upstream_task>, trước <downstream_task>.
Retry: 3 lần, delay 5 phút."
```

---

## 🚀 Quick Start Commands

```bash
# Khởi động toàn bộ platform
make up

# Chạy ETL thủ công
make run-etl

# Chạy dbt transform
make dbt-run

# Xem logs
make logs

# Chạy tests
make test
```

---

## 📊 Deploy Decision

| Option | Khi nào chọn | Chi phí ước tính |
|---|---|---|
| **Local only** | Dev/test, data < 10GB | Free |
| **VPS (Hetzner/DigitalOcean)** | Production nhỏ, budget thấp | ~$20-50/tháng |
| **AWS (RDS + EC2)** | Scale, team > 1 người | ~$100-300/tháng |
| **Azure Synapse** | Đã dùng Microsoft stack | ~$200+/tháng |

**Recommendation:** Bắt đầu Local → VPS khi ổn định → Cloud khi cần scale.

---

*Last updated: 2026 | Project: retail-data-platform | Owner: Dung*
