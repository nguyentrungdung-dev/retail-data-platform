-- Staging model cho đơn hàng
-- Clean, cast types, tính revenue và profit

with source as (
    select * from {{ source('raw', 'raw_orders') }}
),

cleaned as (
    select
        -- Keys
        order_id,
        product_code,
        coalesce(customer_id, 'UNKNOWN')    as customer_id,

        -- Dates
        order_date::timestamp               as order_date,
        order_date::date                    as order_date_day,
        extract(year  from order_date)::int as order_year,
        extract(month from order_date)::int as order_month,
        extract(dow   from order_date)::int as order_dow,  -- 0=Sun, 6=Sat

        -- Product info
        product_name,

        -- Metrics
        qty_sold::numeric(10,2)             as qty_sold,
        selling_price::numeric(15,2)        as selling_price,
        cost_price::numeric(15,2)           as cost_price,
        coalesce(discount_amount, 0)::numeric(15,2) as discount_amount,

        -- Calculated
        (qty_sold * selling_price - coalesce(discount_amount, 0))
                                            as revenue,
        (qty_sold * selling_price - coalesce(discount_amount, 0))
            - (qty_sold * coalesce(cost_price, 0))
                                            as gross_profit,

        -- Dimensions
        coalesce(payment_method, 'unknown') as payment_method,
        coalesce(order_type, 'retail')      as order_type,
        coalesce(source_system, 'unknown')  as source_system,
        staff_id,
        notes,

        -- Metadata
        ingested_at

    from source
    where order_id is not null
      and order_date is not null
      and qty_sold > 0
      and selling_price > 0
),

final as (
    select
        *,
        -- Gross margin %
        case
            when revenue > 0
            then round(gross_profit / revenue * 100, 2)
            else 0
        end as gross_margin_pct,

        -- Surrogate key
        md5(order_id || '|' || product_code) as order_line_key

    from cleaned
)

select * from final
