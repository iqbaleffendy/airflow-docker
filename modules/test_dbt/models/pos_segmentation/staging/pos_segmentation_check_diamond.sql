select
    *,
    case when total_diamond > 2 then 'Diamond' else 'Not Diamond' end is_diamond
from (
    select 
        pos_code,
        concat(date_part(year, month_year_date), '-', date_part(quarter, month_year_date)) as quarter_real,
        concat(date_part(year,dateadd(quarter, 1, month_year_date)), '-', date_part(quarter,dateadd(quarter, 1, month_year_date))) as quarter_next,
        count(distinct month_year) as total_diamond
    from {{source('table_source', 'lp_pos_segmentation')}} 
    where revenue_tiering = 'Diamond'
    and to_char(month_year_date, 'yyyy-MM') in (select * from {{ref('filter_month')}})
    group by 1,2,3
)