-- Hiệu suất sản phẩm trong 90 ngày gần nhất
-- Dùng cho: ranking sản phẩm, phát hiện hàng chậm

with orders as (
    select * from {{ ref('stg_orders') }}
    where order_date >= current_date - interval '90 days'
),

products as (
    select * from {{ ref('stg_products') }}
),

product_sales as (
    select
        o.product_code,
        p.product_name,
        p.category_l1,
        p.category_l2,
        p.brand,

        -- Volume
        count(distinct o.order_id)              as total_orders,
        sum(o.qty_sold)                         as total_qty_sold,

        -- Revenue & Profit
        sum(o.revenue)                          as total_revenue,
        sum(o.gross_profit)                     as total_gross_profit,
        round(avg(o.gross_margin_pct), 2)       as avg_margin_pct,

        -- Dates
        min(o.order_date_day)                   as first_sale_date,
        max(o.order_date_day)                   as last_sale_date,
        current_date - max(o.order_date_day)    as days_since_last_sale,

        -- Average daily sales
        round(sum(o.qty_sold) / 90.0, 2)        as avg_daily_qty

    from orders o
    left join products p using (product_code)
    group by 1, 2, 3, 4, 5
),

final as (
    select
        *,
        -- Ranking
        rank() over (order by total_revenue desc)     as revenue_rank,
        rank() over (order by total_qty_sold desc)    as qty_rank,
        rank() over (order by total_gross_profit desc) as profit_rank,

        -- Nhãn tốc độ bán
        case
            when days_since_last_sale > 60  then 'CHẬM BÁN'
            when avg_daily_qty >= 1         then 'BÁN CHẠY'
            when avg_daily_qty >= 0.3       then 'BÌNH THƯỜNG'
            else                                 'CHẬM'
        end                                     as sales_velocity

    from product_sales
)

select * from final
order by revenue_rank
