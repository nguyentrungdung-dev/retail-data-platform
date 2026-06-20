-- Phân tích mùa vụ (seasonality) cho từng sản phẩm
--
-- Output: với mỗi (product_code, month), index mùa vụ:
--   seasonal_index > 1.0  → tháng đó bán hơn mức trung bình
--   seasonal_index < 1.0  → tháng đó bán dưới mức trung bình
--   seasonal_index = 1.5  → tháng đó bán cao gấp 1.5 lần trung bình
--
-- Dùng cho:
--   - Lập kế hoạch nhập hàng theo mùa
--   - Diagnose forecast của Prophet (có khớp seasonality cũ không)
--   - Dashboard "Sản phẩm bán mạnh tháng nào"

{{ config(materialized = 'table') }}

with history as (
    select * from {{ ref('int_demand_history') }}
    -- Cần ít nhất 1 năm data để tính seasonality có ý nghĩa
    where ds >= current_date - interval '730 days'
),

-- Trung bình hàng tháng theo SKU
monthly_avg as (
    select
        product_code,
        month,
        avg(qty_sold)                       as avg_qty_in_month,
        sum(qty_sold)                       as total_qty_in_month,
        count(distinct year)                as years_observed
    from history
    group by 1, 2
),

-- Trung bình tháng tổng thể của SKU (baseline để so sánh)
overall_avg as (
    select
        product_code,
        avg(qty_sold)                       as avg_qty_per_day_overall,
        sum(qty_sold)                       as total_qty_overall
    from history
    group by 1
),

-- Tính seasonal index = avg tháng N / avg tháng tổng thể
indexed as (
    select
        m.product_code,
        m.month,
        m.years_observed,
        round(m.avg_qty_in_month::numeric, 2)               as avg_qty_in_month,
        round(o.avg_qty_per_day_overall::numeric, 2)        as avg_qty_overall,
        case
            when o.avg_qty_per_day_overall > 0
            then round(
                (m.avg_qty_in_month / o.avg_qty_per_day_overall)::numeric,
                2
            )
            else 1.0
        end                                 as seasonal_index
    from monthly_avg m
    join overall_avg o using (product_code)
),

-- Year-over-year growth: so sánh tháng N năm nay với năm trước
yoy as (
    select
        product_code,
        year,
        month,
        sum(qty_sold)                       as qty_this_year
    from history
    group by 1, 2, 3
),

yoy_compared as (
    select
        product_code,
        year,
        month,
        qty_this_year,
        lag(qty_this_year) over (
            partition by product_code, month
            order by year
        )                                   as qty_last_year
    from yoy
),

yoy_growth as (
    select
        product_code,
        month,
        avg(
            case
                when qty_last_year > 0
                then (qty_this_year - qty_last_year) * 100.0 / qty_last_year
                else null
            end
        )::numeric(10,2)                    as avg_yoy_growth_pct
    from yoy_compared
    where qty_last_year is not null
    group by 1, 2
),

final as (
    select
        i.product_code,
        i.month,

        -- Tên tháng tiếng Việt cho dễ đọc
        case i.month
            when  1 then 'Tháng 1'
            when  2 then 'Tháng 2 (Tết)'
            when  3 then 'Tháng 3'
            when  4 then 'Tháng 4'
            when  5 then 'Tháng 5'
            when  6 then 'Tháng 6 (Hè)'
            when  7 then 'Tháng 7 (Hè)'
            when  8 then 'Tháng 8'
            when  9 then 'Tháng 9'
            when 10 then 'Tháng 10'
            when 11 then 'Tháng 11'
            when 12 then 'Tháng 12 (Cuối năm)'
        end                                 as month_label,

        i.avg_qty_in_month,
        i.avg_qty_overall,
        i.seasonal_index,

        -- Phân loại
        case
            when i.seasonal_index >= 1.5 then 'PEAK'         -- Đỉnh mùa
            when i.seasonal_index >= 1.2 then 'HIGH'         -- Cao
            when i.seasonal_index >= 0.8 then 'NORMAL'       -- Bình thường
            when i.seasonal_index >= 0.5 then 'LOW'          -- Thấp
            else                              'OFF_SEASON'   -- Ngoài mùa
        end                                 as season_status,

        i.years_observed,
        coalesce(y.avg_yoy_growth_pct, 0)   as avg_yoy_growth_pct
    from indexed i
    left join yoy_growth y using (product_code, month)
)

select * from final
order by product_code, month
