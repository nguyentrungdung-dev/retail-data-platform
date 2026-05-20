-- Phân khúc khách hàng theo RFM
-- R = Recency (mua gần đây chưa?)
-- F = Frequency (mua bao nhiêu lần?)
-- M = Monetary (tổng tiền bao nhiêu?)

with orders as (
    select * from {{ ref('stg_orders') }}
    where order_date >= current_date - interval '365 days'
),

-- Tổng hợp metrics từng khách
customer_metrics as (
    select
        customer_id,
        count(distinct order_id)                as order_count,
        sum(revenue)                            as total_revenue,
        avg(revenue)                            as avg_order_value,
        min(order_date_day)                     as first_order_date,
        max(order_date_day)                     as last_order_date,
        current_date - max(order_date_day)      as recency_days,
        -- Sản phẩm hay mua nhất
        mode() within group (order by product_code) as favorite_product

    from orders
    where customer_id != 'UNKNOWN'
    group by 1
),

-- Tính RFM scores (1-5)
rfm_scores as (
    select
        *,
        -- R Score: càng mua gần đây càng cao
        case
            when recency_days <= 30  then 5
            when recency_days <= 60  then 4
            when recency_days <= 90  then 3
            when recency_days <= 180 then 2
            else                          1
        end as r_score,

        -- F Score: càng mua nhiều lần càng cao
        case
            when order_count >= 12 then 5
            when order_count >= 8  then 4
            when order_count >= 4  then 3
            when order_count >= 2  then 2
            else                        1
        end as f_score,

        -- M Score: càng chi nhiều tiền càng cao
        case
            when total_revenue >= 50000000 then 5
            when total_revenue >= 20000000 then 4
            when total_revenue >= 10000000 then 3
            when total_revenue >= 5000000  then 2
            else                                1
        end as m_score

    from customer_metrics
),

-- Gán nhãn phân khúc
segments as (
    select
        *,
        (r_score + f_score + m_score) as rfm_total,
        case
            when r_score >= 4 and f_score >= 4 and m_score >= 4
                then 'CHAMPIONS'          -- Khách VIP, mua thường xuyên
            when f_score >= 4 and m_score >= 3
                then 'LOYAL'              -- Khách trung thành
            when r_score >= 4 and f_score = 1
                then 'NEW CUSTOMER'       -- Khách mới
            when r_score >= 3 and f_score >= 3
                then 'POTENTIAL'          -- Tiềm năng
            when r_score <= 2 and f_score >= 3
                then 'AT RISK'            -- Nguy cơ mất khách
            when r_score = 1 and f_score >= 2
                then 'LOST'               -- Đã mất
            else
                'OTHERS'
        end as segment

    from rfm_scores
)

select
    customer_id,
    segment,
    r_score,
    f_score,
    m_score,
    rfm_total,
    order_count,
    round(total_revenue)        as total_revenue,
    round(avg_order_value)      as avg_order_value,
    recency_days,
    first_order_date,
    last_order_date,
    favorite_product,
    -- Flag VIP
    total_revenue >= 20000000   as is_vip

from segments
order by rfm_total desc
