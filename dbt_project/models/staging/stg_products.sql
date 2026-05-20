-- Staging model cho sản phẩm

with source as (
    select * from {{ source('raw', 'raw_products') }}
),

final as (
    select
        product_code,
        product_name,
        coalesce(category_l1, 'Chưa phân loại') as category_l1,
        coalesce(category_l2, 'Chưa phân loại') as category_l2,
        coalesce(brand, 'Không rõ')             as brand,
        coalesce(unit, 'cái')                   as unit,
        cost_price::numeric(15,2)               as cost_price,
        list_price::numeric(15,2)               as list_price,
        -- Biên lợi nhuận catalog
        case
            when list_price > 0
            then round((list_price - cost_price) / list_price * 100, 2)
            else 0
        end                                     as catalog_margin_pct,
        coalesce(is_active, true)               as is_active,
        supplier_code,
        ingested_at
    from source
    where product_code is not null
)

select * from final
