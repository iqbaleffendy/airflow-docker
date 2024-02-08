with unioned_data as (
    select * from {{ref('pos_segmentation_stg')}} 
    union all
    select * from {{ref('pos_segmentation_no_trx_temp')}} 
)

select  
    *,
    case
        when previous_tiering = 'G/P' and pos_tiering = 'G/P' then 'Active G/P'
        when previous_tiering = 'G/P' and pos_tiering = 'Regular' then 'Downgrade to Regular'
        when previous_tiering = 'G/P' and pos_tiering = 'No Trx' then 'No Trx G/P'
        when previous_tiering = 'NEW' and pos_tiering = 'G/P' then 'New G/P'
        when previous_tiering = 'Regular' and pos_tiering = 'G/P' then 'Upgrade to G/P'
        when previous_tiering = 'No Trx' and pos_tiering = 'G/P' then 'Back Active to G/P'
        when previous_tiering = 'Regular' and pos_tiering = 'Regular' then 'Active Regular'
        when previous_tiering = 'Regular' and pos_tiering = 'No Trx' then 'No Trx Regular'
        when previous_tiering = 'NEW' and pos_tiering = 'Regular' then 'New Regular'
        when previous_tiering = 'No Trx' and pos_tiering = 'Regular' then 'Back Active to Regular'
        when previous_tiering = 'No Trx' and pos_tiering = 'No Trx' then 'Churn'
        else null
    end as tiering_changes,
    current_timestamp as etl_date
from (
    select
        *,
        lag(pos_tiering, 1) over (partition by pos_code order by month_year_date) as previous_tiering,
        lag(real_pos_tiering, 1) over (partition by pos_code order by month_year_date) as previous_real_tiering,
        lag(revenue, 1) over (partition by pos_code order by month_year_date) as previous_revenue
    from unioned_data 
) where month_year = '{{var("month_year")}}'