-- Mart Demand Forecast — kết hợp output Prophet với data thực tế
--
-- Logic:
--   1. Chỉ lấy run mới nhất (latest run_id, status=SUCCESS)
--   2. Join với stg_products để có category, brand
--   3. Join với int_product_seasonality để biết tháng nào là mùa cao điểm
--   4. Tính các chỉ số derived: forecast_vs_seasonal_baseline, suggested_reorder
--
-- Dùng cho:
--   - Dashboard "Dự báo nhu cầu 90 ngày tới"
--   - Báo cáo tự động: SKU nào nên nhập thêm tuần tới
--   - So sánh forecast với seasonal index (sanity check)

{{ config(materialized = 'table') }}

with latest_run as (
    -- Lấy run_id mới nhất đã SUCCESS
    select run_id
    from {{ source('raw', 'raw_forecast_runs') }}
    where status = 'SUCCESS'
    order by run_finished_at desc
    limit 1
),

forecasts as (
    select
        f.product_code,
        f.ds,
        f.yhat,
        f.yhat_lower,
        f.yhat_upper,
        f.is_actual,
        f.actual_qty
    from {{ source('raw', 'raw_forecast_results') }} f
    inner join latest_run lr on f.run_id = lr.run_id
),

products as (
    select * from {{ ref('stg_products') }}
),

seasonality as (
    select
        product_code,
        month,
        seasonal_index,
        season_status
    from {{ ref('int_product_seasonality') }}
),

-- Hiện tại tồn kho: lấy snapshot mới nhất
current_stock as (
    select
        product_code,
        qty_on_hand,
        snapshot_date
    from {{ source('raw', 'raw_inventory') }}
    where snapshot_date = (
        select max(snapshot_date) from {{ source('raw', 'raw_inventory') }}
    )
),

-- Daily forecast với context đầy đủ
daily as (
    select
        f.product_code,
        f.ds,
        extract(month from f.ds)::int           as forecast_month,

        -- Forecast values
        round(f.yhat, 2)                        as forecast_qty,
        round(f.yhat_lower, 2)                  as forecast_qty_lower,
        round(f.yhat_upper, 2)                  as forecast_qty_upper,

        -- Confidence width (rộng = không chắc chắn, hẹp = tự tin)
        round(f.yhat_upper - f.yhat_lower, 2)   as confidence_width,

        f.is_actual,
        f.actual_qty,

        -- Sai số (nếu có actual)
        case
            when f.is_actual and f.actual_qty > 0
            then round(abs(f.yhat - f.actual_qty) / f.actual_qty * 100, 2)
            else null
        end                                     as ape_pct,        -- Absolute % error

        -- Product context
        p.product_name,
        p.category_l1,
        p.category_l2,
        p.brand,
        p.list_price,

        -- Seasonality context
        coalesce(s.seasonal_index, 1.0)         as seasonal_index,
        coalesce(s.season_status, 'NORMAL')     as season_status,

        -- Inventory context
        coalesce(cs.qty_on_hand, 0)             as current_stock_qty
    from forecasts f
    left join products p
        on f.product_code = p.product_code
    left join seasonality s
        on f.product_code = s.product_code
       and extract(month from f.ds)::int = s.month
    left join current_stock cs
        on f.product_code = cs.product_code
),

-- Aggregate theo SKU + horizon (next 30/60/90 days) cho dashboard
sku_summary as (
    select
        product_code,
        product_name,
        category_l1,
        category_l2,
        brand,
        list_price,
        current_stock_qty,

        -- Tổng forecast 30/60/90 ngày tới
        sum(case when ds between current_date and current_date + interval '30 days'
                 and not is_actual
                 then forecast_qty else 0 end)          as forecast_qty_30d,
        sum(case when ds between current_date and current_date + interval '60 days'
                 and not is_actual
                 then forecast_qty else 0 end)          as forecast_qty_60d,
        sum(case when ds between current_date and current_date + interval '90 days'
                 and not is_actual
                 then forecast_qty else 0 end)          as forecast_qty_90d,

        -- Cận trên (worst-case demand) cho safety stock planning
        sum(case when ds between current_date and current_date + interval '30 days'
                 and not is_actual
                 then forecast_qty_upper else 0 end)    as forecast_upper_30d,

        -- Average MAPE trên data historical (để biết model tin được không)
        avg(ape_pct)                                    as backtest_mape
    from daily
    group by 1, 2, 3, 4, 5, 6, 7
),

final as (
    select
        product_code,
        product_name,
        category_l1,
        category_l2,
        brand,
        list_price,
        current_stock_qty,

        round(forecast_qty_30d, 2)                          as forecast_qty_30d,
        round(forecast_qty_60d, 2)                          as forecast_qty_60d,
        round(forecast_qty_90d, 2)                          as forecast_qty_90d,
        round(forecast_upper_30d, 2)                        as forecast_upper_30d,

        -- Recommendation: cần nhập thêm bao nhiêu để đủ 30 ngày
        greatest(
            ceil(forecast_qty_30d - current_stock_qty),
            0
        )                                                   as suggested_reorder_qty,

        -- Safety stock: dùng cận trên (95th percentile demand) - current
        greatest(
            ceil(forecast_upper_30d - current_stock_qty),
            0
        )                                                   as safety_reorder_qty,

        -- Days of stock theo forecast (current / avg daily forecast)
        case
            when forecast_qty_30d > 0
            then round(current_stock_qty / (forecast_qty_30d / 30.0), 0)
            else null
        end                                                 as days_of_stock,

        round(backtest_mape, 2)                             as backtest_mape_pct,

        -- Trust level dựa trên MAPE backtest
        case
            when backtest_mape <= 15  then 'HIGH'           -- < 15% sai số
            when backtest_mape <= 30  then 'MEDIUM'
            when backtest_mape <= 50  then 'LOW'
            else                           'UNRELIABLE'
        end                                                 as forecast_trust,

        -- Trạng thái cảnh báo
        case
            when current_stock_qty = 0
                 and forecast_qty_30d > 0
                then 'OUT_OF_STOCK'                         -- Hết hàng nhưng có nhu cầu
            when current_stock_qty < forecast_qty_30d * 0.3
                then 'CRITICAL_LOW'                         -- < 30% nhu cầu
            when current_stock_qty < forecast_qty_30d
                then 'NEED_REORDER'                         -- < 100% nhu cầu
            when current_stock_qty > forecast_qty_90d * 1.5
                then 'OVERSTOCK'                            -- Quá nhu cầu 90 ngày × 1.5
            else 'ADEQUATE'
        end                                                 as stock_alert
    from sku_summary
)

select * from final
order by suggested_reorder_qty desc, forecast_qty_30d desc
