-- Customer-level metrics base table
--
-- Tổng hợp mọi chỉ số đo lường về 1 khách hàng trong 12 tháng gần nhất
-- và 12 tháng trước đó (để so sánh, phát hiện churn).
-- Là input chung cho: mart_rfm, mart_customer_360.

{{ config(materialized = 'table') }}

with orders as (
    select * from {{ ref('stg_orders') }}
    where customer_id != 'UNKNOWN'
),

-- Cửa sổ phân tích chính: 365 ngày gần nhất
window_current as (
    select * from orders
    where order_date_day >= current_date - interval '365 days'
),

-- Cửa sổ so sánh: 365–730 ngày trước (period trước đó)
window_previous as (
    select * from orders
    where order_date_day >= current_date - interval '730 days'
      and order_date_day <  current_date - interval '365 days'
),

-- Metrics period hiện tại
metrics_current as (
    select
        customer_id,
        count(distinct order_id)                    as order_count,
        count(distinct order_date_day)              as active_days,
        sum(qty_sold)                               as total_qty,
        sum(revenue)                                as total_revenue,
        sum(gross_profit)                           as total_gross_profit,
        avg(revenue)                                as avg_order_value,
        max(revenue)                                as max_order_value,

        min(order_date_day)                         as first_order_date,
        max(order_date_day)                         as last_order_date,
        current_date - max(order_date_day)          as recency_days,

        max(order_date_day) - min(order_date_day) + 1   as customer_lifetime_days,

        mode() within group (order by product_code) as favorite_product,
        mode() within group (order by order_type)   as primary_order_type,
        mode() within group (order by payment_method) as primary_payment

    from window_current
    group by 1
),

-- Metrics period trước đó (chỉ cần volume + revenue để tính delta)
metrics_previous as (
    select
        customer_id,
        count(distinct order_id)    as prev_order_count,
        sum(revenue)                as prev_total_revenue
    from window_previous
    group by 1
),

-- Tần suất mua: ngày trung bình giữa 2 đơn (inter-purchase time)
purchase_intervals as (
    select
        customer_id,
        avg(days_between)::numeric(10,2) as avg_days_between_orders,
        stddev(days_between)::numeric(10,2) as stddev_days_between_orders
    from (
        select
            customer_id,
            order_date_day - lag(order_date_day) over (
                partition by customer_id order by order_date_day
            ) as days_between
        from (
            select distinct customer_id, order_date_day
            from window_current
        ) t
    ) gaps
    where days_between is not null
    group by 1
),

final as (
    select
        c.customer_id,

        -- Recency / Frequency / Monetary base
        c.recency_days,
        c.order_count,
        c.total_revenue,

        -- Mở rộng
        c.active_days,
        c.total_qty,
        c.total_gross_profit,
        round(c.avg_order_value)                            as avg_order_value,
        round(c.max_order_value)                            as max_order_value,

        c.first_order_date,
        c.last_order_date,
        c.customer_lifetime_days,

        -- Tần suất mua (NULL nếu chỉ mua 1 lần)
        coalesce(pi.avg_days_between_orders, null)          as avg_days_between_orders,
        coalesce(pi.stddev_days_between_orders, null)       as stddev_days_between_orders,

        -- Margin
        case
            when c.total_revenue > 0
            then round(c.total_gross_profit / c.total_revenue * 100, 2)
            else 0
        end                                                 as gross_margin_pct,

        -- So sánh với period trước
        coalesce(p.prev_order_count, 0)                     as prev_order_count,
        coalesce(p.prev_total_revenue, 0)                   as prev_total_revenue,

        case
            when coalesce(p.prev_total_revenue, 0) > 0
            then round(
                (c.total_revenue - p.prev_total_revenue)
                / p.prev_total_revenue * 100, 2
            )
            else null
        end                                                 as revenue_yoy_pct,

        -- Dimensions thống kê
        c.favorite_product,
        c.primary_order_type,
        c.primary_payment,

        -- Phân loại nhanh theo order_type chiếm đa số
        case
            when c.primary_order_type in ('wholesale', 'contractor') then 'B2B'
            else 'B2C'
        end                                                 as customer_segment_b2b_b2c

    from metrics_current c
    left join metrics_previous p using (customer_id)
    left join purchase_intervals pi using (customer_id)
)

select * from final
