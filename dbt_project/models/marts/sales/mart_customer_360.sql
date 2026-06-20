-- Mart Customer 360 — góc nhìn toàn cảnh từng khách hàng
--
-- Mở rộng mart_rfm với:
--   1. Churn probability (xác suất rời đi) — dựa trên recency vs avg_interval
--   2. Engagement score 0-100 — composite từ R/F/M
--   3. Customer health status (HEALTHY / WATCHING / AT_RISK / CHURNED)
--   4. Days since last order so với expected next order
--
-- Dùng cho: dashboard CRM, list mục tiêu marketing, ưu tiên CSKH gọi điện.

{{ config(materialized = 'table') }}

with rfm as (
    select * from {{ ref('mart_rfm') }}
),

base as (
    select * from {{ ref('int_customer_metrics') }}
),

-- Join để lấy đủ thông tin từ cả 2 nguồn
joined as (
    select
        r.customer_id,
        r.segment,
        r.rfm_code,
        r.r_score, r.f_score, r.m_score, r.rfm_total,

        r.order_count,
        r.total_revenue,
        r.total_gross_profit,
        r.gross_margin_pct,
        r.avg_order_value,
        r.max_order_value,

        r.recency_days,
        r.avg_days_between_orders,
        b.stddev_days_between_orders,
        r.first_order_date,
        r.last_order_date,
        r.customer_lifetime_days,

        r.predicted_clv_2y,
        r.revenue_yoy_pct,
        r.favorite_product,
        r.customer_segment_b2b_b2c,
        r.next_best_action,
        r.is_vip,
        r.is_new,
        r.needs_winback,

        -- Số ngày kỳ vọng giữa 2 đơn dựa trên hành vi cá nhân khách
        -- (KHÔNG dùng global average vì mỗi khách có chu kỳ khác nhau)
        coalesce(r.avg_days_between_orders, 90)         as expected_interval_days
    from rfm r
    left join base b using (customer_id)
),

-- Tính churn probability:
-- Logic: nếu recency > 2 lần khoảng cách trung bình → churn cao
-- Sigmoid-like mapping qua case-when (xấp xỉ logistic curve)
churn_calc as (
    select
        *,
        recency_days::numeric / nullif(expected_interval_days, 0)
            as recency_ratio,

        case
            -- Khách chỉ mua 1 lần: dùng ngưỡng cứng theo recency
            when order_count = 1 then
                case
                    when recency_days <= 30  then 0.10
                    when recency_days <= 60  then 0.25
                    when recency_days <= 90  then 0.45
                    when recency_days <= 180 then 0.70
                    else                          0.90
                end
            -- Khách mua ≥ 2 lần: so với chu kỳ cá nhân
            when recency_days <= expected_interval_days * 0.5  then 0.05
            when recency_days <= expected_interval_days * 1.0  then 0.15
            when recency_days <= expected_interval_days * 1.5  then 0.35
            when recency_days <= expected_interval_days * 2.0  then 0.55
            when recency_days <= expected_interval_days * 3.0  then 0.75
            when recency_days <= expected_interval_days * 5.0  then 0.90
            else                                                    0.97
        end                                             as churn_probability
    from joined
),

-- Engagement Score (0-100): composite weighted score
-- 50% R, 25% F, 25% M để ưu tiên hành vi gần đây
engagement as (
    select
        *,
        round(
            (r_score * 10 * 0.5)        -- max 25
            + (f_score * 10 * 0.25)     -- max 12.5
            + (m_score * 10 * 0.25)     -- max 12.5
            * 2                          -- scale to 0-100
        )::int                                          as engagement_score
    from churn_calc
),

-- Health status — 4 mức trực quan để dashboard hiển thị màu sắc
health as (
    select
        *,
        case
            when churn_probability >= 0.85  then 'CHURNED'
            when churn_probability >= 0.50  then 'AT_RISK'
            when churn_probability >= 0.25  then 'WATCHING'
            else                                 'HEALTHY'
        end                                             as health_status,

        -- Ngày dự kiến đơn hàng tiếp theo (best guess)
        last_order_date + (expected_interval_days || ' days')::interval
            as expected_next_order_date,

        -- Số ngày quá hạn so với kỳ vọng
        greatest(
            recency_days - expected_interval_days,
            0
        )                                               as days_overdue
    from engagement
),

-- Priority score for CRM/marketing list:
-- Cao = ưu tiên cao (đáng winback hoặc đáng giữ)
priority as (
    select
        *,
        case
            -- VIP đang at-risk → ưu tiên cao nhất
            when is_vip and health_status in ('AT_RISK', 'WATCHING')
                then 100

            -- VIP healthy → giữ tốt, ưu tiên trung bình cao
            when is_vip and health_status = 'HEALTHY'
                then 80

            -- Khách lớn but đã churn → cứu vớt nếu được
            when m_score >= 4 and health_status = 'CHURNED'
                then 70

            -- Khách mới có dấu hiệu tốt → đẩy mạnh nurturing
            when is_new and m_score >= 3
                then 65

            -- Trung bình at-risk
            when health_status = 'AT_RISK'
                then 50

            -- Khách mới giá trị thấp
            when is_new
                then 35

            -- Healthy thấp
            when health_status = 'HEALTHY'
                then 30

            -- Lost low-value: bỏ qua
            else 10
        end                                             as crm_priority_score
    from health
)

select
    customer_id,

    -- Segmentation
    segment,
    health_status,
    rfm_code,
    r_score, f_score, m_score, rfm_total,

    -- Engagement & Risk scores
    engagement_score,
    round(churn_probability::numeric, 4)                as churn_probability,
    crm_priority_score,

    -- Volume
    order_count,
    round(total_revenue)                                as total_revenue,
    round(total_gross_profit)                           as total_gross_profit,
    avg_order_value,
    max_order_value,
    gross_margin_pct,

    -- Behavior
    recency_days,
    expected_interval_days,
    days_overdue,
    expected_next_order_date,
    avg_days_between_orders,
    stddev_days_between_orders,
    first_order_date,
    last_order_date,
    customer_lifetime_days,

    -- Value prediction
    predicted_clv_2y,
    revenue_yoy_pct,

    -- Dimensions
    customer_segment_b2b_b2c,
    favorite_product,

    -- Actions
    next_best_action,

    -- Flags
    is_vip,
    is_new,
    needs_winback

from priority
order by crm_priority_score desc, total_revenue desc
