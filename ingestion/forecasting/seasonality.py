"""
Helpers cho Demand Forecasting — feature mùa vụ Việt Nam.

Module này KHÔNG phụ thuộc Prophet, có thể dùng độc lập:
  - vietnamese_holidays(): trả về DataFrame holidays cho Prophet
  - is_high_season_month(): check tháng có phải mùa cao điểm
  - get_vn_seasons(): map tháng → tên mùa retail VN

Lý do ngày lễ VN khác Prophet built-in:
  - Tết âm lịch lệch dương lịch mỗi năm → phải hardcode
  - Mid-Autumn (Trung thu), Black Friday VN, ngày Phụ nữ 20/10... cũng cần.
"""
from __future__ import annotations

from datetime import date
from typing import Iterable

import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
# TẾT ÂM LỊCH (ngày dương lịch của mùng 1 Tết)
# Source: lichvansu.com — cần update khi qua từng năm
# Format: year → (start_date, end_date) — bao 7 ngày Tết kéo dài
# ─────────────────────────────────────────────────────────────────────────────
LUNAR_NEW_YEAR: dict[int, tuple[str, str]] = {
    2022: ("2022-01-29", "2022-02-06"),
    2023: ("2023-01-20", "2023-01-28"),
    2024: ("2024-02-08", "2024-02-16"),
    2025: ("2025-01-25", "2025-02-02"),
    2026: ("2026-02-15", "2026-02-23"),
    2027: ("2027-02-04", "2027-02-12"),
    2028: ("2028-01-24", "2028-02-01"),
}

# Ngày lễ dương lịch cố định (MM-DD): impact retail trung bình - cao
FIXED_HOLIDAYS_MMDD: dict[str, str] = {
    "01-01": "Tết Dương lịch",
    "04-30": "Giải phóng miền Nam",
    "05-01": "Quốc tế Lao động",
    "09-02": "Quốc khánh",
    "03-08": "8/3 — Phụ nữ",
    "10-20": "20/10 — Phụ nữ VN",
    "11-20": "Nhà giáo VN",
    "12-25": "Giáng Sinh",
    # Sale event (impact mạnh đến retail điện máy):
    "11-11": "Sale 11/11",
    "12-12": "Sale 12/12",
}

# Mid-Autumn (Trung thu) — rằm tháng 8 âm lịch, cũng phải hardcode dương lịch
MID_AUTUMN: dict[int, str] = {
    2022: "2022-09-10",
    2023: "2023-09-29",
    2024: "2024-09-17",
    2025: "2025-10-06",
    2026: "2026-09-25",
    2027: "2027-09-15",
    2028: "2028-10-03",
}

# Black Friday VN (thứ 6 cuối tháng 11) — sự kiện sale lớn cho điện máy
BLACK_FRIDAY: dict[int, str] = {
    2022: "2022-11-25",
    2023: "2023-11-24",
    2024: "2024-11-29",
    2025: "2025-11-28",
    2026: "2026-11-27",
    2027: "2027-11-26",
    2028: "2028-11-24",
}


def vietnamese_holidays(years: Iterable[int] | None = None) -> pd.DataFrame:
    """
    Trả về DataFrame holidays format chuẩn Prophet.

    Schema:
      ds            datetime64
      holiday       str         tên ngày lễ
      lower_window  int         số ngày trước ds bị ảnh hưởng (âm)
      upper_window  int         số ngày sau ds bị ảnh hưởng

    Args:
        years: list năm cần generate, mặc định 2022-2028.

    Example:
        >>> df = vietnamese_holidays([2024, 2025])
        >>> Prophet(holidays=df)
    """
    if years is None:
        years = range(2022, 2029)

    rows: list[dict] = []

    for year in years:
        # Tết âm lịch: window rộng (-7, +7) vì impact kéo dài
        if year in LUNAR_NEW_YEAR:
            start, _end = LUNAR_NEW_YEAR[year]
            rows.append({
                "ds": pd.Timestamp(start),
                "holiday": "tet_lunar_new_year",
                "lower_window": -7,
                "upper_window": 7,
            })

        # Trung thu: window (-3, +1) — bán đồ tặng trẻ em, bánh, đèn
        if year in MID_AUTUMN:
            rows.append({
                "ds": pd.Timestamp(MID_AUTUMN[year]),
                "holiday": "mid_autumn",
                "lower_window": -3,
                "upper_window": 1,
            })

        # Black Friday: window (-1, +2)
        if year in BLACK_FRIDAY:
            rows.append({
                "ds": pd.Timestamp(BLACK_FRIDAY[year]),
                "holiday": "black_friday",
                "lower_window": -1,
                "upper_window": 2,
            })

        # Ngày lễ cố định
        for mmdd, name in FIXED_HOLIDAYS_MMDD.items():
            month, day = map(int, mmdd.split("-"))
            try:
                ds = pd.Timestamp(year=year, month=month, day=day)
            except ValueError:
                continue
            rows.append({
                "ds": ds,
                "holiday": _slugify(name),
                "lower_window": -1,
                "upper_window": 1,
            })

    df = pd.DataFrame(rows)
    return df.sort_values("ds").reset_index(drop=True)


def _slugify(text: str) -> str:
    """Chuyển tên ngày lễ tiếng Việt → slug ascii dùng làm holiday name."""
    import unicodedata

    nfkd = unicodedata.normalize("NFKD", text)
    ascii_text = "".join(c for c in nfkd if not unicodedata.combining(c))
    return (
        ascii_text.lower()
        .replace(" ", "_")
        .replace("/", "_")
        .replace("—", "")
        .replace("-", "_")
        .strip("_")
    )


def is_high_season_month(month: int) -> bool:
    """
    True nếu tháng đó là mùa cao điểm retail VN cho điện máy/nội thất:
      - Tháng 1-2: Tết, mua sắm cuối năm
      - Tháng 5-7: Hè (máy lạnh, tủ lạnh)
      - Tháng 11-12: Sale 11/11, Black Friday, cuối năm
    """
    return month in {1, 2, 5, 6, 7, 11, 12}


def get_vn_season(month: int) -> str:
    """Map tháng → mùa retail VN (dùng cho dashboard, không cho Prophet)."""
    seasons = {
        1: "TẾT",      2: "TẾT",
        3: "BÌNH THƯỜNG",
        4: "BÌNH THƯỜNG",
        5: "HÈ",       6: "HÈ",       7: "HÈ",
        8: "BÌNH THƯỜNG",
        9: "TRUNG THU",
        10: "BÌNH THƯỜNG",
        11: "CUỐI NĂM (Sale)",
        12: "CUỐI NĂM (Sale)",
    }
    return seasons.get(month, "BÌNH THƯỜNG")


def in_lunar_new_year(d: date | pd.Timestamp) -> bool:
    """True nếu ngày `d` rơi vào khoảng Tết âm lịch."""
    if isinstance(d, pd.Timestamp):
        d = d.date()

    year = d.year
    if year not in LUNAR_NEW_YEAR:
        return False

    start_str, end_str = LUNAR_NEW_YEAR[year]
    start = date.fromisoformat(start_str)
    end = date.fromisoformat(end_str)
    return start <= d <= end
