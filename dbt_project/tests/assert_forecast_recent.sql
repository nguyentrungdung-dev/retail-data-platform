-- Test: forecast mới nhất phải được chạy trong vòng 14 ngày qua
-- Nếu lâu hơn → DAG weekly_forecast bị hỏng, cảnh báo ngay
--
-- LƯU Ý: skip nếu chưa có run nào (fresh deploy chưa chạy DAG).
--        Logic: chỉ fail khi có run cũ nhưng đã quá 14 ngày.

with last_run as (
    select max(run_finished_at) as last_successful_run
    from {{ source('raw', 'raw_forecast_runs') }}
    where status = 'SUCCESS'
)

select
    last_successful_run,
    current_date - last_successful_run::date    as days_ago
from last_run
where last_successful_run is not null
  and current_date - last_successful_run::date > 14
