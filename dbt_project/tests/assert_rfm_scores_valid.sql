-- Test: RFM scores phải trong khoảng 1-5

select customer_id, r_score, f_score, m_score
from {{ ref('mart_rfm') }}
where r_score not between 1 and 5
   or f_score not between 1 and 5
   or m_score not between 1 and 5
