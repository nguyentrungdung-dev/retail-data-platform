-- Test: gross margin không được vượt quá 80% (hoặc < -10%)
-- Nếu vượt ngưỡng → thường do nhập sai giá vốn / chiết khấu

select
    order_id,
    product_code,
    gross_margin_pct
from {{ ref('stg_orders') }}
where gross_margin_pct > 80
   or gross_margin_pct < -10
