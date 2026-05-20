-- Doanh thu tổng hợp theo ngày
-- Dùng cho: biểu đồ trend, so sánh tháng này vs tháng trước

with orders as (
    select * from {{ ref('stg_orders') }}
),

daily as (
    select
        order_date_day                          as date,
        order_year,
        order_month,
        order_type,

        -- Volume
        count(distinct order_id)                as total_orders,
        count(distinct customer_id)             as unique_customers,
        sum(qty_sold)                           as total_qty,

        -- Revenue
        sum(revenue)                            as total_revenue,
        sum(gross_profit)                       as total_gross_profit,
        round(avg(gross_margin_pct), 2)         as avg_margin_pct,

        -- Average
        round(sum(revenue) /
            nullif(count(distinct order_id), 0), 0)
                                                as avg_order_value

    from orders
    group by 1, 2, 3, 4
),

-- So sánh với ngày hôm qua
final as (
    select
        *,
        lag(total_revenue) over (
            partition by order_type
            order by date
        )                                       as prev_day_revenue,

        round(
            (total_revenue - lag(total_revenue) over (
                partition by order_type order by date
            )) / nullif(lag(total_revenue) over (
                partition by order_type order by date
            ), 0) * 100
        , 2)                                    as revenue_growth_pct

    from daily
)

select * from final
order by date desc
