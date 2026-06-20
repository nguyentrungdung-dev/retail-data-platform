-- Lịch sử nhu cầu (demand) hàng ngày theo SKU
--
-- Đây là input cho Prophet forecast: cần daily time series sạch, không bị thiếu ngày.
-- Logic:
--   1. Tổng hợp qty_sold theo (product_code, order_date_day)
--   2. Spine date × SKU để fill ngày không có đơn → qty = 0
--   3. Loại SKU quá ít data (< 30 ngày có doanh số) — Prophet không học được

{{ config(materialized = 'table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
),

-- 1. Aggregate raw daily demand
daily_raw as (
    select
        product_code,
        order_date_day                  as ds,
        sum(qty_sold)                   as qty_sold,
        sum(revenue)                    as revenue,
        count(distinct order_id)        as order_count
    from orders
    group by 1, 2
),

-- 2. Tìm SKU đủ data để forecast
-- Ngưỡng min_sales_days config qua dbt var:
--   - prod: 30 ngày (default) — đảm bảo Prophet học được seasonality
--   - dev/demo: 7 ngày — chấp nhận MAPE cao để demo workflow
-- Override khi chạy: dbt build --vars '{min_sales_days: 7}'
eligible_skus as (
    select
        product_code,
        count(*)                        as days_with_sales,
        min(ds)                         as first_sale_date,
        max(ds)                         as last_sale_date
    from daily_raw
    group by 1
    having count(*) >= {{ var('min_sales_days', 30) }}
),

-- 3. Date spine: từ ngày đầu tiên có data đến hôm nay
date_spine as (
    select
        d::date as ds
    from generate_series(
        (select min(first_sale_date) from eligible_skus),
        current_date,
        interval '1 day'
    ) as d
),

-- 4. Cross join SKU × date → filter theo first/last_sale_date của SKU đó
sku_date_grid as (
    select
        e.product_code,
        s.ds,
        e.first_sale_date,
        e.last_sale_date
    from eligible_skus e
    cross join date_spine s
    -- Chỉ tính từ ngày SKU đó bắt đầu bán đến hôm nay (không tính tương lai)
    where s.ds >= e.first_sale_date
),

-- 5. Fill 0 cho ngày không có đơn
final as (
    select
        g.product_code,
        g.ds,
        coalesce(d.qty_sold, 0)         as qty_sold,
        coalesce(d.revenue, 0)          as revenue,
        coalesce(d.order_count, 0)      as order_count,

        -- Date features (Prophet cần ds, y; còn lại là extra regressors nếu cần)
        extract(year  from g.ds)::int   as year,
        extract(month from g.ds)::int   as month,
        extract(day   from g.ds)::int   as day_of_month,
        extract(dow   from g.ds)::int   as day_of_week,
        extract(week  from g.ds)::int   as iso_week,

        -- Flag cuối tuần (Sat/Sun) — quan trọng cho retail VN
        case when extract(dow from g.ds) in (0, 6) then 1 else 0 end
                                        as is_weekend,

        -- Flag ngày từ first_sale_date → cô lập SKU mới ra mắt
        g.ds - g.first_sale_date        as days_since_launch
    from sku_date_grid g
    left join daily_raw d
        on g.product_code = d.product_code
       and g.ds = d.ds
)

select * from final
