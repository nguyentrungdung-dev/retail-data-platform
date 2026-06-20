-- Phân khúc khách hàng theo RFM (NTILE quantile-based)
--
-- Khác bản cũ (rule-based với threshold cứng): bản này dùng NTILE(5) chia
-- khách thành 5 nhóm bằng nhau theo distribution thực tế của data.
-- → Tự thích ứng khi data tăng/giảm, không cần tune thủ công.
--
-- R = Recency (mua gần đây chưa?) — càng nhỏ càng tốt
-- F = Frequency (mua bao nhiêu lần?) — càng lớn càng tốt
-- M = Monetary (tổng tiền bao nhiêu?) — càng lớn càng tốt

{{ config(materialized = 'table') }}

with base as (
    select * from {{ ref('int_customer_metrics') }}
),

-- Tính NTILE 1-5 cho từng metric
-- LƯU Ý: với recency, NTILE thấp = khách mới mua gần đây (tốt) nên đảo dấu
ntiles as (
    select
        *,
        ntile(5) over (order by recency_days asc)       as r_quintile,  -- 1=mới nhất
        ntile(5) over (order by order_count asc)        as f_quintile,  -- 1=ít nhất
        ntile(5) over (order by total_revenue asc)      as m_quintile   -- 1=thấp nhất
    from base
),

scored as (
    select
        *,
        -- Đảo R: khách càng mới mua thì score càng cao (5)
        (6 - r_quintile)                                as r_score,
        f_quintile                                      as f_score,
        m_quintile                                      as m_score,
        -- Composite RFM score (chuẩn 3 chữ số, vd '545')
        (6 - r_quintile) * 100
        + f_quintile * 10
        + m_quintile                                    as rfm_code
    from ntiles
),

-- Predicted CLV (Customer Lifetime Value) đơn giản
-- Công thức: AOV × Purchase Frequency × Predicted Lifespan
-- Predicted Lifespan = 365 / avg_days_between_orders × 2 năm (giả định retain 2 năm)
clv_calc as (
    select
        *,
        case
            when order_count >= 2 and avg_days_between_orders > 0
                then round(
                    avg_order_value
                    * (365.0 / avg_days_between_orders)
                    * 2.0
                )
            -- Khách mới (1 đơn): dự báo dựa trên giá trị đơn đầu × hệ số 1.5
            else round(avg_order_value * 1.5)
        end                                             as predicted_clv_2y
    from scored
),

-- Gán segment dựa trên (R, F, M) score
-- Bộ rule chuẩn của RFM analytics, mở rộng từ bản cũ
segments as (
    select
        *,
        case
            -- Top tier
            when r_score = 5 and f_score >= 4 and m_score >= 4
                then 'CHAMPIONS'              -- Khách VIP, mua thường xuyên & chi nhiều
            when r_score >= 4 and f_score >= 4 and m_score >= 3
                then 'LOYAL'                  -- Khách trung thành
            when r_score = 5 and f_score = 1
                then 'NEW_CUSTOMER'           -- Khách mới mua lần đầu

            -- Mid tier
            when r_score >= 4 and f_score >= 2 and m_score >= 3
                then 'POTENTIAL_LOYAL'        -- Tiềm năng trở thành VIP
            when r_score >= 3 and f_score >= 2 and m_score >= 2
                then 'PROMISING'              -- Có tiềm năng nhưng chưa rõ
            when r_score >= 4 and f_score = 1 and m_score = 1
                then 'RECENT_LOW_VALUE'       -- Mới nhưng giá trị thấp

            -- Need attention
            when r_score = 3 and f_score = 3 and m_score = 3
                then 'NEED_ATTENTION'         -- Trung bình mọi mặt → cần kích cầu
            when r_score <= 2 and f_score >= 4 and m_score >= 4
                then 'CANT_LOSE'              -- Từng VIP, lâu rồi không mua
            when r_score <= 2 and f_score >= 2 and m_score >= 3
                then 'AT_RISK'                -- Có nguy cơ rời đi

            -- Lost
            when r_score = 1 and f_score >= 2
                then 'HIBERNATING'            -- Ngủ đông, có thể wake up
            when r_score = 1 and f_score = 1
                then 'LOST'                   -- Mất hẳn

            else 'OTHERS'
        end                                             as segment
    from clv_calc
),

-- Gán Next Best Action gợi ý cho từng segment
nba as (
    select
        *,
        case segment
            when 'CHAMPIONS'        then 'Tri ân: voucher VIP, ưu tiên hàng mới'
            when 'LOYAL'            then 'Upsell: gợi ý sản phẩm cao cấp hơn'
            when 'NEW_CUSTOMER'     then 'Onboarding: hướng dẫn, mời mua lần 2'
            when 'POTENTIAL_LOYAL'  then 'Khuyến khích: mua đủ N đơn được giảm'
            when 'PROMISING'        then 'Cross-sell: giới thiệu sản phẩm bổ trợ'
            when 'RECENT_LOW_VALUE' then 'Educate: gợi ý sản phẩm chính của shop'
            when 'NEED_ATTENTION'   then 'Re-engage: gửi voucher 5-10%'
            when 'CANT_LOSE'        then 'Win-back KHẨN CẤP: gọi điện trực tiếp'
            when 'AT_RISK'          then 'Win-back: voucher 10-15%, follow-up'
            when 'HIBERNATING'      then 'Awake: campaign tổng + voucher mạnh'
            when 'LOST'             then 'Re-acquire: chi phí thấp, list cuối'
            else                         'Khảo sát thêm'
        end                                             as next_best_action
    from segments
)

select
    customer_id,

    -- Phân khúc & scoring
    segment,
    rfm_code,
    r_score,
    f_score,
    m_score,
    (r_score + f_score + m_score)               as rfm_total,

    -- Volume metrics
    order_count,
    total_qty,
    round(total_revenue)                        as total_revenue,
    round(total_gross_profit)                   as total_gross_profit,
    avg_order_value,
    max_order_value,
    gross_margin_pct,

    -- Behavior
    recency_days,
    avg_days_between_orders,
    first_order_date,
    last_order_date,
    customer_lifetime_days,

    -- Predicted value
    predicted_clv_2y,

    -- YoY
    revenue_yoy_pct,

    -- Dimensions
    favorite_product,
    customer_segment_b2b_b2c,

    -- Action
    next_best_action,

    -- Flags
    (m_score >= 4 and total_revenue >= 20000000)    as is_vip,
    (segment in ('AT_RISK', 'CANT_LOSE'))           as needs_winback,
    (segment = 'NEW_CUSTOMER')                      as is_new

from nba
order by rfm_total desc, total_revenue desc
