-- Test: revenue phải luôn >= 0
-- Nếu query này trả về rows → test FAIL

select
    order_id,
    product_code,
    revenue
from {{ ref('stg_orders') }}
where revenue < 0
