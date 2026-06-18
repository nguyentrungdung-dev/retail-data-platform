# Metabase Dashboards — Auto-provision

6 dashboard chính cho retail-data-platform, được định nghĩa dạng YAML và provision qua Metabase REST API.

## Cấu trúc

```
dashboards/metabase/
├── README.md            ← file này
├── provision.py         ← script đẩy YAML → Metabase
├── requirements.txt
└── queries/
    ├── 01_doanh_thu.yml         ← Doanh thu, trend, B2C/B2B
    ├── 02_top_san_pham.yml      ← Top SP theo revenue/qty/category/brand
    ├── 03_ton_kho.yml           ← Hết hàng, sắp hết, cần nhập
    ├── 04_khach_vip.yml         ← VIP, RFM segments, AT RISK
    ├── 05_hang_cham.yml         ← CHẬM BÁN / HÀNG CHẾT
    └── 06_b2b_contractor.yml    ← Doanh thu sỉ/contractor, top KH B2B
```

Mỗi YAML định nghĩa 1 dashboard với 4-5 cards: SQL native query, kiểu chart, vị trí grid.

## Setup lần đầu

### Bước 1 — Setup wizard Metabase (chỉ chạy 1 lần)

1. Đảm bảo stack đang chạy: `docker compose up -d`
2. Mở http://localhost:3000 → bấm "Hãy bắt đầu"
3. Điền tên + email + password của bạn (ghi nhớ — sẽ dùng cho `.env`)
4. **Add database**:
   - Type: **PostgreSQL**
   - Display name: `retail_dw` (BẮT BUỘC dùng tên này, hoặc set `MB_DATABASE` trong `.env`)
   - Host: `postgres-dwh` (DNS nội bộ docker network)
   - Port: `5432` (KHÔNG dùng 5434 — đó là port host-mapped)
   - Database name: `retail_dw`
   - User: `retail`
   - Password: `retail123`
5. Bỏ qua các bước còn lại của wizard.

### Bước 2 — Điền `.env`

```
MB_URL=http://localhost:3000
MB_USER=email_admin_bạn_vừa_tạo@example.com
MB_PASSWORD=password_bạn_vừa_tạo
MB_DATABASE=retail_dw
```

### Bước 3 — Cài deps + chạy provision

```bash
# Tạo venv riêng cho tools (khuyến khích)
python3 -m venv .venv-tools
source .venv-tools/bin/activate
pip install -r dashboards/metabase/requirements.txt

# Validate YAML trước (không gọi API, an toàn)
python dashboards/metabase/provision.py --dry-run

# Provision thật
python dashboards/metabase/provision.py
```

Script in từng dashboard và URL trực tiếp khi xong:

```
🛠  Provisioning '1. Doanh thu' ...
      + card 'Doanh thu ngày gần nhất' (id=12)
      + card 'Trend doanh thu 30 ngày' (id=13)
      ...
   ✅ Dashboard id=4 → http://localhost:3000/dashboard/4
```

## Vận hành

### Sửa 1 card

Edit trực tiếp YAML trong `queries/` rồi chạy:

```bash
python dashboards/metabase/provision.py --replace
```

Flag `--replace` archive dashboard cũ trùng tên (giữ history, không xóa hẳn) rồi tạo mới.

### Thêm dashboard mới

Tạo thêm file YAML trong `queries/` (đặt prefix số tăng dần: `07_xxx.yml`). Provision sẽ tự pick up.

### Xóa dashboard

Vào Metabase UI → 3 dấu chấm → Move to trash. Hoặc archive thủ công.

## YAML schema

```yaml
name: "Tên dashboard"
description: "Mô tả ngắn"
cards:
  - name: "Tên card"
    visualization: scalar | line | bar | row | table | pie | area
    position: { row: 0, col: 0, size_x: 12, size_y: 4 }   # grid 18 cột
    viz_settings: {}                                      # optional Metabase viz settings
    sql: |
      select ...
```

**Lưu ý grid**: Metabase dùng grid 18 cột (mặc định). `size_x` tối đa 18 (full width), `size_y` cao bao nhiêu hàng tùy ý. `row` và `col` 0-indexed.

## Troubleshooting

| Lỗi | Nguyên nhân | Fix |
|---|---|---|
| `Login Metabase thất bại (401)` | Sai `MB_USER` / `MB_PASSWORD` | Xem lại `.env`, đảm bảo email khớp với account đã tạo lúc setup wizard |
| `Không tìm thấy database 'retail_dw'` | Chưa add database trong Metabase, hoặc tên khác | Vào http://localhost:3000/admin/databases. Hoặc set `MB_DATABASE=<tên_đã_đặt>` trong `.env` |
| `Connection refused` khi script chạy | Metabase chưa lên, hoặc đang trên VPS khác | `docker compose ps`. Nếu trên VPS: set `MB_URL=http://<ip>:3000` |
| Dashboard tạo ra nhưng card lỗi `Native query has not been supported` | SQL syntax sai hoặc reference table chưa tồn tại | Chạy `dbt build` trước. Hoặc test SQL bằng `psql` thẳng: `docker compose exec postgres-dwh psql -U retail -d retail_dw -c "<query>"` |
