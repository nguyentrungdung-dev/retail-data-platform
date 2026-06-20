# 🏪 Retail Data Platform

> Modern data platform for Vietnamese retail stores (electronics, home appliances, furniture).
> Phân tích hành vi mua hàng, tồn kho thông minh, dự báo nhu cầu mùa vụ.

[![Python](https://img.shields.io/badge/Python-3.11-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white)](https://www.postgresql.org/)
[![dbt](https://img.shields.io/badge/dbt-1.7.4-FF694B?logo=dbt&logoColor=white)](https://www.getdbt.com/)
[![Airflow](https://img.shields.io/badge/Airflow-2.9.1-017CEE?logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Tests](https://img.shields.io/badge/dbt%20tests-97%20passing-brightgreen)]()
[![Tests](https://img.shields.io/badge/pytest-25%20passing-brightgreen)]()

---

## 📌 Mục tiêu

Trả lời 3 câu hỏi business cho cửa hàng điện máy / nội thất tỉnh lẻ Việt Nam:

| Câu hỏi | Mart trả lời |
|---|---|
| Sản phẩm nào bán chạy / chậm theo mùa, khu vực? | `mart_product_performance`, `mart_demand_forecast`, `int_product_seasonality` |
| Khách nào có giá trị cao? Ai sắp rời đi (churn)? | `mart_customer_360`, `mart_rfm` |
| Tồn kho nào đang thừa / thiếu so với nhu cầu thực? | `mart_inventory_health`, `mart_demand_forecast` |

Khách hàng mục tiêu: **B2C bán lẻ** + **B2B contractors** (nhà thầu, đại lý).

---

## 🏗 Architecture

```
┌──────────────────┐
│  Data Sources    │
│  • KiotViet API  │ ◄── OAuth2 polling 15 phút (incremental)
│  • Excel files   │ ◄── Fallback khi API down
│  • Manual entry  │
└────────┬─────────┘
         │
         ▼
┌──────────────────────────────────────────┐
│  Ingestion Layer  (Python + pandas)      │
│  • OAuth2 token cache (23h)              │
│  • Rate limiting (55 req/phút)           │
│  • Retry: 401 / 429 / 5xx                │
│  • _sync_state tracking incremental      │
└────────┬─────────────────────────────────┘
         │ upsert
         ▼
┌──────────────────────────────────────────┐
│  PostgreSQL 15 — Data Warehouse          │
│  raw_* → stg_* → int_* → mart_*          │
└────────┬─────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────┐
│  Transform Layer  (dbt Core)             │
│  • 11 models, 86 tests, lineage tracked  │
│  • Star schema (fct + dim)               │
│  • dbt_utils macros                      │
└────────┬─────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────┐
│  ML Layer  (Facebook Prophet)            │
│  • Demand forecast 90 ngày tới           │
│  • VN holidays (Tết, Trung thu, BlackFr) │
│  • Customer churn probability            │
└────────┬─────────────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────┐
│  Consumption  (Metabase + Power BI)      │
└──────────────────────────────────────────┘
         ▲
         │
┌────────┴─────────────────────────────────┐
│  Orchestration  (Apache Airflow 2.9)     │
│  • daily_etl              02:00 hàng ngày │
│  • kiotviet_polling      */15 phút        │
│  • weekly_forecast       03:00 Chủ Nhật   │
└──────────────────────────────────────────┘
```

---

## 🛠 Tech Stack

| Layer | Tool | Lý do chọn |
|---|---|---|
| Ingestion | Python 3.11, pandas, requests, SQLAlchemy 2.x | Linh hoạt, ecosystem rộng |
| Database | PostgreSQL 15 | Mạnh, free, SQL chuẩn |
| Data Lake | MinIO (S3-compatible) | On-prem, free |
| Transform | dbt Core 1.7.4 | SQL-first, có test, lineage |
| Orchestration | Apache Airflow 2.9 | Phổ biến, nhiều tài liệu |
| ML | Facebook Prophet 1.1.5 | Handle seasonality + holidays tốt |
| BI | Metabase + Power BI | Metabase self-serve, Power BI cho report cao cấp |
| Infra | Docker Compose | Chạy local → dễ migrate cloud |

---

## ✨ Features

### 📊 Bước 1-11: Foundation
- ✅ Docker Compose 8 services (Postgres DWH, Airflow, Metabase, MinIO, pgAdmin, …)
- ✅ Excel ingestion với column auto-mapping (tiếng Việt + tiếng Anh)
- ✅ Idempotent upsert qua `_row_hash` (insert / update / skip)
- ✅ dbt staging + intermediate + marts theo star schema
- ✅ Email + Zalo notifications (fail-safe nếu chưa config)

### 🧠 Bước 12: Advanced Analytics
- ✅ **RFM nâng cao**: NTILE quantile-based (tự thích ứng với data), 12 segments
- ✅ **Customer 360**: CLV 2 năm, churn probability, engagement score, CRM priority
- ✅ **Next Best Action** mỗi segment (Win-back, Onboarding, Upsell, …)
- ✅ **Demand Forecasting** với Prophet: 90 ngày, multiplicative seasonality
- ✅ **VN holidays** chuẩn: Tết âm lịch (window -7/+7), Trung thu, Black Friday, 8/3, 20/10, 11/11, 12/12
- ✅ **Reorder suggestion** + safety stock + stock alert (5 mức)
- ✅ **MAPE backtest** → trust level (HIGH/MEDIUM/LOW/UNRELIABLE)

### 🔌 Bước 13: KiotViet Integration
- ✅ OAuth2 client credentials flow + token cache 23h
- ✅ HTTP client: retry, exponential backoff, rate limit sliding window
- ✅ Incremental sync với `lastModifiedFrom` + `_sync_state` audit table
- ✅ 5 entities: invoices, orders, products, customers, inventory
- ✅ Polling DAG mỗi 15 phút (near real-time)
- ✅ Skip dbt build nếu không có row nào upsert (tiết kiệm tài nguyên)

---

## 🚀 Quick Start

### Yêu cầu
- Docker Desktop ≥ 4.0 (Apple Silicon hoặc Intel)
- 8 GB RAM trống tối thiểu
- Python 3.11 (chỉ cần nếu develop ngoài container)

### 1. Clone & cấu hình
```bash
git clone https://github.com/<your-username>/retail-data-platform.git
cd retail-data-platform

cp .env.example .env

vim .env
```

### 2. Khởi động toàn bộ stack
```bash
docker compose up -d

docker ps
```

| Service | URL | Login |
|---|---|---|
| Airflow | http://localhost:8080 | `admin` / `admin` |
| Metabase | http://localhost:3000 | Setup wizard lần đầu |
| pgAdmin | http://localhost:5050 | `admin@retail.com` / `admin` |
| MinIO | http://localhost:9001 | `minioadmin` / `minioadmin123` |
| PostgreSQL | localhost:5434 | từ `.env` |

### 3. Apply migrations (lần đầu)
```bash
docker exec -i retail_dwh psql -U retail -d retail_dw \
  < infra/migrations/001_forecast_tables.sql

docker exec -i retail_dwh psql -U retail -d retail_dw \
  < infra/migrations/002_sync_state.sql
```

### 4. Chạy thử pipeline với sample data
```bash
docker compose exec airflow-scheduler airflow dags trigger retail_daily_etl

docker exec retail_dwh psql -U retail -d retail_dw \
  -c "SELECT count(*) FROM mart_customer_360;"
```

### 5. Kết nối KiotViet (production)
Xem chi tiết: [docs/kiotviet_setup.md](docs/kiotviet_setup.md)

---

## 📂 Project Structure

```
retail-data-platform/
├── CLAUDE.md                   ← AI agent context (đọc trước mọi thứ)
├── docker-compose.yml          ← 8 services
├── Dockerfile.airflow          ← Custom image: Airflow + dbt-venv
│
├── ingestion/                  ← Python ETL
│   ├── config.py
│   ├── sources/
│   │   ├── kiotviet/           ← KiotViet API package (auth, client, extractors, transformers, runner)
│   │   ├── excel.py
│   │   └── manual_entry.py
│   ├── forecasting/            ← Prophet ML pipeline
│   │   ├── prophet_forecast.py
│   │   ├── seasonality.py      ← VN holidays
│   │   └── runner.py
│   └── loaders/
│       ├── postgres_loader.py
│       └── sync_state.py       ← Incremental tracking
│
├── dbt_project/                ← dbt transformations
│   ├── models/
│   │   ├── staging/            ← stg_orders, stg_products
│   │   ├── intermediate/       ← int_customer_metrics, int_demand_history, int_product_seasonality
│   │   └── marts/
│   │       ├── sales/          ← mart_sales_daily, mart_rfm, mart_customer_360, mart_demand_forecast, mart_product_performance
│   │       └── inventory/      ← mart_inventory_health
│   ├── tests/                  ← Custom tests
│   └── packages.yml            ← dbt_utils 1.1.1
│
├── dags/                       ← Airflow DAGs
│   ├── daily_etl.py            ← Daily 02:00 — Excel + dbt build
│   ├── kiotviet_polling_etl.py ← Mỗi 15' — sync KiotViet incremental
│   └── weekly_forecast.py      ← Chủ Nhật 03:00 — Prophet forecast 90d
│
├── infra/
│   ├── init_db.sql             ← DDL khởi tạo lần đầu
│   └── migrations/             ← Versioned migrations
│       ├── 001_forecast_tables.sql
│       └── 002_sync_state.sql
│
├── tests/                      ← Python pytest
│   └── test_kiotviet_transformers.py  ← 25 tests
│
├── docs/
│   └── kiotviet_setup.md       ← Hướng dẫn 9 sections
│
└── dashboards/
    ├── metabase/               ← Dashboard JSON exports
    └── power_bi/               ← .pbix files
```

---

## 🗄 Data Model

### Layered architecture

```
raw_*         ← dữ liệu gốc, append-only, có _row_hash để detect changes
  ↓
stg_*         ← cleaned, typed, calculated columns (revenue, gross_profit, …)
  ↓
int_*         ← intermediate models (customer metrics, demand history, seasonality)
  ↓
mart_*        ← aggregated cho BI dashboards
```

### Marts chính

| Mart | Granularity | Use case |
|---|---|---|
| `mart_sales_daily` | 1 row / (date, order_type) | Trend doanh thu, growth YoY |
| `mart_rfm` | 1 row / customer | Phân khúc khách hàng (12 segments) |
| `mart_customer_360` | 1 row / customer | CRM dashboard, churn alert |
| `mart_product_performance` | 1 row / product | Ranking SKU 90 ngày |
| `mart_inventory_health` | 1 row / product | Cảnh báo nhập hàng |
| `mart_demand_forecast` | 1 row / product | Forecast 30/60/90 ngày + reorder qty |

---

## 📐 Business Rules

```python
doanh_thu_thuan = qty_sold * selling_price - discount_amount
loi_nhuan_gop   = doanh_thu_thuan - (qty_sold * cost_price)

hang_cham_ban     = không có đơn 60+ ngày
hang_het_hang     = qty_on_hand <= reorder_point (default 2)
days_of_stock     = qty_on_hand / avg_daily_sales_30d

khach_vip                = monetary >= 20M VND / 12 tháng
khach_co_nguy_co_churn   = churn_probability >= 0.5
khach_moi                = first_order trong 30 ngày gần nhất
```

---

## 🧪 Tests & Quality

| Suite | Tool | Coverage |
|---|---|---|
| dbt data tests | dbt-core | 86 schema tests + 11 custom tests = **97 tests** |
| Python unit tests | pytest | 25 tests cho transformers (mock data) |
| Lint | flake8 + black + isort | PEP8 compliant |

```bash
docker exec -it retail_airflow_scheduler bash -lc "
  cd /opt/airflow/dbt_project &&
  /opt/dbt-venv/bin/dbt test --profiles-dir . --project-dir .
"

.venv/bin/python -m pytest tests/ -v
```

---

## 📅 DAG Schedule

| DAG | Schedule | Purpose |
|---|---|---|
| `retail_daily_etl` | `0 2 * * *` (02:00 hàng ngày) | Ingest Excel → dbt build all |
| `retail_kiotviet_polling` | `*/15 * * * *` (mỗi 15 phút) | Sync KiotViet incremental |
| `retail_weekly_forecast` | `0 3 * * 0` (03:00 Chủ Nhật) | Prophet forecast 90 ngày |

Mỗi DAG có:
- ✅ Retry với exponential backoff
- ✅ Execution timeout
- ✅ `on_failure_callback` → Email + Zalo notification
- ✅ `max_active_runs=1` (tránh overlap)

---

## 🗺 Roadmap

### ✅ Đã hoàn thành
- [x] Bước 1-11: Foundation (Docker + Postgres + Airflow + dbt + Metabase)
- [x] Bước 12: Advanced analytics (RFM nâng cao + Prophet forecasting)
- [x] Bước 13: KiotViet API integration (polling 15 phút)

### 🚧 Future enhancements
- [ ] Webhook receiver cho real-time < 5s (KiotViet push events)
- [ ] Power BI dataset refresh DAG (`weekly_report.py`)
- [ ] Metabase dashboards as code (provision qua API)
- [ ] Prophet model improvements: exogenous regressors (giá, promotion)
- [ ] Data quality monitoring (Great Expectations / Soda)
- [ ] CI/CD pipeline (GitHub Actions: lint + test + dbt compile)
- [ ] Terraform IaC để migrate lên AWS/GCP
- [ ] Slack notifications (thay Zalo cho team distributed)

---

## 📚 Documentation

- [`CLAUDE.md`](CLAUDE.md) — Project context cho AI agents
- [`docs/kiotviet_setup.md`](docs/kiotviet_setup.md) — Hướng dẫn lấy KiotViet credentials và verify

---

## 🐛 Troubleshooting

| Triệu chứng | Giải pháp |
|---|---|
| `service "airflow-scheduler" is not running` | Dùng `docker exec retail_airflow_scheduler ...` (container_name) hoặc `docker compose exec airflow-scheduler ...` (service name) — đừng trộn lẫn |
| `relation "raw_forecast_runs" does not exist` | Apply migration: `docker exec -i retail_dwh psql ... < infra/migrations/001_forecast_tables.sql` |
| Prophet forecast `NoDataError` | Cần ≥ 30 ngày doanh số / SKU. Override dev: `dbt build --vars '{min_sales_days: 7}'` |
| `KiotVietAuthError: 401` | client_id/secret sai — regenerate trong KiotViet admin |
| pgAdmin 8+ reject email `.local` | Dùng email TLD chuẩn (`.com`/`.vn`) trong env |

---

## 🤝 Contributing

PR welcome. Quy ước:
- Python: PEP8, type hints bắt buộc, docstring tiếng Việt OK
- SQL: lowercase keywords, snake_case names
- Commit message: theo pattern `<type>(<scope>): <description>` (vd: `feat(kiotviet): thêm webhook receiver`)
- Mọi function phải có try/except + logging
- Idempotency: chạy lại pipeline phải cho cùng kết quả

---

## 📄 License

MIT License — xem [LICENSE](LICENSE) để biết chi tiết.

---

## 👤 Author

**Trung Dũng (Dung)**

Project context: nhằm giải quyết bài toán **phân tích hành vi mua hàng và tồn kho thông minh** cho các cửa hàng điện máy / đồ gia dụng / nội thất ở huyện / tỉnh lẻ Việt Nam, phục vụ cả khách hàng B2C và nhà thầu B2B.

---

<sub>Built with ❤️ for Vietnamese retail · 2026</sub>
