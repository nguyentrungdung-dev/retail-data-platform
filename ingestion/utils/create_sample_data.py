"""
Tạo file Excel mẫu để test pipeline.
Chạy: python ingestion/utils/create_sample_data.py

Ràng buộc đảm bảo data hợp lý (không vi phạm test data quality):
- selling_price > cost_price (margin nội tại 15-25%)
- discount = 0-15% của revenue → không gây margin âm sâu
- Kết quả: gross_margin_pct luôn nằm trong [0%, 25%]
"""
import pandas as pd
import random
from datetime import datetime, timedelta

random.seed(42)  # reproducible output

# (mã, tên, selling_price, cost_price) — cost = 75-85% selling → margin 15-25%
products = [
    ("SP001", "Máy lạnh Panasonic 1HP",     8500000,  7000000),
    ("SP002", "Tủ lạnh Samsung 300L",       9200000,  7500000),
    ("SP003", "Máy giặt LG 9kg",            7500000,  6100000),
    ("SP004", "TV Samsung 55 inch 4K",     12000000,  9800000),
    ("SP005", "Nồi cơm điện Sunhouse 1.8L",  890000,   720000),
    ("SP006", "Quạt đứng Panasonic",         650000,   500000),
    ("SP007", "Bàn ăn gỗ 6 ghế",            4500000,  3500000),
    ("SP008", "Tủ quần áo 3 cánh",          3200000,  2500000),
]

customers = [
    ("KH001", "Nguyễn Văn An",         "0901234567", "individual"),
    ("KH002", "Trần Thị Bình",         "0912345678", "individual"),
    ("KH003", "Công ty XD Minh Phát",  "0923456789", "contractor"),
    ("KH004", "Lê Văn Cường",          "0934567890", "individual"),
    ("KH005", "Phạm Thị Dung",         "0945678901", "individual"),
]

# Tỷ lệ discount theo % revenue: phần lớn không khuyến mãi, một số ít có 5-15%
# Tránh discount > 15% để không kéo margin xuống âm
discount_pcts = [0.0, 0.0, 0.0, 0.0, 0.05, 0.10, 0.15]

rows = []
base_date = datetime(2025, 1, 1)

for i in range(150):
    p = random.choice(products)
    c = random.choice(customers)
    date = base_date + timedelta(days=random.randint(0, 364))
    qty = random.randint(1, 3)

    selling_price = p[2]
    cost_price = p[3]

    revenue_before_discount = qty * selling_price
    discount_pct = random.choice(discount_pcts)
    # Round discount về bội số 1000đ cho thực tế
    discount = round(revenue_before_discount * discount_pct / 1000) * 1000

    rows.append({
        "Mã đơn":      f"DH{str(i+1).zfill(4)}",
        "Ngày bán":    date.strftime("%d/%m/%Y"),
        "Mã khách":    c[0],
        "SĐT":         c[2],
        "Mã SP":       p[0],
        "Tên SP":      p[1],
        "Số lượng":    qty,
        "Đơn giá":     selling_price,
        "Giá vốn":     cost_price,
        "Chiết khấu":  discount,
        "Thanh toán":  random.choice(["Tiền mặt", "Chuyển khoản"]),
        "Nhân viên":   random.choice(["NV01", "NV02", "NV03"]),
    })

df = pd.DataFrame(rows)
df.to_excel("data/samples/orders_sample.xlsx", index=False, sheet_name="Đơn hàng")
print(f"✅ Đã tạo {len(df)} rows tại data/samples/orders_sample.xlsx")
