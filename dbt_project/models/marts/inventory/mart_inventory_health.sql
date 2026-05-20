-- Tình trạng sức khỏe tồn kho
-- Dùng cho: cảnh báo nhập hàng, xử lý hàng chậm

with inventory as (
    -- Lấy snapshot tồn kho mới nhất
    select * from {{ source('raw', 'raw_inventory') }}
    where snapshot_date = (
        select max(snapshot_date) from {{ source('raw', 'raw_inventory') }}
    )
),

products as (
    select * from {{ ref('stg_products') }}
),

-- Tốc độ bán 30 và 90 ngày
sales_velocity as (
    select
        product_code,
        -- 30 ngày
        sum(case when order_date >= current_date - 30
            then qty_sold else 0 end) / 30.0   as avg_daily_sales_30d,
        -- 90 ngày
        sum(case when order_date >= current_date - 90
            then qty_sold else 0 end) / 90.0   as avg_daily_sales_90d,
        -- Lần bán gần nhất
        max(order_date_day)                    as last_sale_date,
        current_date - max(order_date_day)     as days_since_last_sale

    from {{ ref('stg_orders') }}
    group by 1
),

joined as (
    select
        i.product_code,
        p.product_name,
        p.category_l1,
        p.brand,
        i.snapshot_date,
        i.qty_on_hand,
        i.qty_reserved,
        i.qty_on_hand - coalesce(i.qty_reserved, 0) as qty_available,

        coalesce(v.avg_daily_sales_30d, 0)     as avg_daily_sales_30d,
        coalesce(v.avg_daily_sales_90d, 0)     as avg_daily_sales_90d,
        v.last_sale_date,
        coalesce(v.days_since_last_sale, 999)  as days_since_last_sale,

        -- Tồn kho đủ dùng bao nhiêu ngày
        case
            when coalesce(v.avg_daily_sales_30d, 0) > 0
            then round(i.qty_on_hand / v.avg_daily_sales_30d)
            else null
        end                                    as days_of_stock

    from inventory i
    left join products p using (product_code)
    left join sales_velocity v using (product_code)
),

final as (
    select
        *,
        -- Trạng thái tồn kho
        case
            when qty_on_hand = 0
                then 'HẾT HÀNG'
            when days_of_stock < 7
                then 'SẮP HẾT'
            when days_of_stock < 14
                then 'CẦN NHẬP'
            when days_since_last_sale > 180 and qty_on_hand > 0
                then 'HÀNG CHẾT'
            when days_since_last_sale > 60 and qty_on_hand > 0
                then 'CHẬM BÁN'
            else
                'BÌNH THƯỜNG'
        end                                    as stock_status,

        -- Gợi ý nhập hàng
        days_of_stock < 14 or qty_on_hand = 0 as need_reorder

    from joined
)

select * from final
order by
    case stock_status
        when 'HẾT HÀNG'  then 1
        when 'SẮP HẾT'   then 2
        when 'CẦN NHẬP'  then 3
        when 'CHẬM BÁN'  then 4
        when 'HÀNG CHẾT' then 5
        else                  6
    end
