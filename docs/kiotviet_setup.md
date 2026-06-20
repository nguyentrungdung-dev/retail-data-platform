# KiotViet API — Setup Guide

Hướng dẫn chi tiết để kết nối KiotViet với Retail Data Platform.

---

## 1. Lấy credentials từ KiotViet

### Bước 1.1: Đăng nhập admin KiotViet
- URL: `https://<retailer>.kiotviet.vn/man/`
- Tài khoản phải có role **Owner** hoặc **Admin** mới tạo được app

### Bước 1.2: Tạo ứng dụng API
1. Vào **Cài đặt** → **API**
2. Click **+ Tạo ứng dụng** (hoặc **Quản lý ứng dụng**)
3. Điền thông tin:
   - **Tên ứng dụng**: `Retail Data Platform`
   - **Mô tả**: `Pull data về DWH cho phân tích`
   - **Public API URL**: `https://public.kiotapi.com`
4. Tích chọn **PublicApi.Access** (scope cần thiết)
5. Click **Lưu**

### Bước 1.3: Copy 3 thông tin sau
| Field | Vị trí | Ví dụ |
|---|---|---|
| `client_id` | Hiển thị sau khi tạo app | `abc123-def-456...` |
| `client_secret` | Hiển thị 1 lần duy nhất ngay sau khi tạo app | `xyz789...` |
| `retailer` | Subdomain trong URL: `<retailer>.kiotviet.vn` | `shopcuaban` |

> ⚠️ **`client_secret` chỉ hiện 1 lần.** Nếu lỡ đóng tab, phải tạo lại app mới.

---

## 2. Cấu hình `.env`

Mở file `.env` ở thư mục root project, điền:

```bash
KIOTVIET_CLIENT_ID=abc123-def-456...
KIOTVIET_CLIENT_SECRET=xyz789...
KIOTVIET_RETAILER=shopcuaban

KIOTVIET_BRANCH_IDS=

KIOTVIET_INITIAL_LOOKBACK_DAYS=30

KIOTVIET_PAGE_SIZE=100
KIOTVIET_REQUEST_TIMEOUT=30
```

---

## 3. Apply migration

Bảng `_sync_state` và `_sync_history` cần tồn tại trước khi sync:

```bash
docker exec -i retail_dwh psql -U retail -d retail_dw \
  < infra/migrations/002_sync_state.sql
```

Verify:

```bash
docker exec retail_dwh psql -U retail -d retail_dw \
  -c "\dt _sync*"
```

Kỳ vọng thấy 2 bảng `_sync_state` và `_sync_history`.

---

## 4. Test kết nối

### 4.1 Test auth (lấy token)

```bash
docker exec -it retail_airflow_scheduler bash -lc "
  cd /opt/airflow &&
  python -c '
from ingestion.sources.kiotviet.auth import get_default_token_manager
mgr = get_default_token_manager()
token = mgr.get_token()
print(f\"Token OK, expires_at={token.expires_at}\")
  '
"
```

Kỳ vọng: `[kiotviet.auth] ✅ Token lấy thành công, expires in ...`

### 4.2 Test sync 1 entity

```bash
docker exec -it retail_airflow_scheduler bash -lc "
  cd /opt/airflow &&
  python -m ingestion.sources.kiotviet.runner --entities products
"
```

Kỳ vọng:

```
[kiotviet.auth] ✅ Token lấy thành công, expires in 86400s (24h)
[sync] BEGIN kiotviet.products window=[...]
[extract] products: 234 rows
[transform] 234 products → 234 rows
[postgres] ✅ inserted=234, updated=0, skipped=0
[sync] ✅ DONE kiotviet.products: fetched=234, upserted=234
```

### 4.3 Verify data trong DB

```bash
docker exec retail_dwh psql -U retail -d retail_dw \
  -c "SELECT count(*) FROM raw_products WHERE source_system='kiotviet';"

docker exec retail_dwh psql -U retail -d retail_dw \
  -c "SELECT * FROM _sync_state ORDER BY entity_name;"
```

---

## 5. Trigger DAG

Bật DAG trên Airflow UI hoặc CLI:

```bash
docker exec retail_airflow_scheduler airflow dags unpause retail_kiotviet_polling

docker exec retail_airflow_scheduler airflow dags trigger retail_kiotviet_polling
```

DAG sẽ tự chạy mỗi 15 phút từ đó.

---

## 6. Full refresh (nếu cần)

Khi nghi ngờ data lệch, force re-sync 1 năm gần nhất:

```bash
docker exec -it retail_airflow_scheduler bash -lc "
  cd /opt/airflow &&
  python -m ingestion.sources.kiotviet.runner --full
"
```

Lệnh này reset `_sync_state.last_synced_at` về 1 năm trước → lần sync tiếp theo kéo lùi.

---

## 7. Troubleshooting

| Triệu chứng | Nguyên nhân | Cách fix |
|---|---|---|
| `KiotVietAuthError: 401 Unauthorized` | client_id/secret sai | Kiểm tra lại, regenerate nếu cần |
| `KIOTVIET_RETAILER chưa được set` | Thiếu env var | Sửa `.env`, restart Airflow |
| Sync chạy nhưng 0 rows | `lastModifiedFrom` quá gần | Chạy `--full` 1 lần |
| `429 rate limited` lặp lại | Có DAG khác cũng poll API | Check `*/15` schedule |
| `_sync_state.last_run_status='FAILED'` | Lần sync gần nhất lỗi | Xem `error_message` cùng row |

---

## 8. Monitoring

### Xem 10 lần sync gần nhất
```sql
SELECT
    entity_name,
    started_at,
    status,
    rows_fetched,
    rows_upserted,
    duration_ms,
    error_message
FROM _sync_history
ORDER BY started_at DESC
LIMIT 10;
```

### Xem trạng thái mới nhất từng entity
```sql
SELECT * FROM _sync_state ORDER BY entity_name;
```

### Đếm rows mới được sync trong 1 giờ qua
```sql
SELECT
    entity_name,
    SUM(rows_upserted) AS upserted_last_hour
FROM _sync_history
WHERE started_at >= NOW() - INTERVAL '1 hour'
  AND status = 'SUCCESS'
GROUP BY entity_name
ORDER BY upserted_last_hour DESC;
```

---

## 9. Bước tiếp theo: Webhook (optional)

Polling 15 phút đủ cho hầu hết use case retail. Nếu cần real-time đúng nghĩa
(< 5 giây), bạn có thể bổ sung webhook receiver. Cần:

1. Public URL (domain hoặc ngrok cho dev)
2. Endpoint nhận POST từ KiotViet
3. Xử lý payload `{event: 'invoice.update', data: {...}}`

Đây là roadmap **phase 2**, chưa làm trong project hiện tại.
