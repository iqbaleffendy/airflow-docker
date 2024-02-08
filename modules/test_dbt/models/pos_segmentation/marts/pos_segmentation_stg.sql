with data_previous_month as (
    select
        month_year,
        month_year_date,
        pos_code,
        revenue,
        pos_aging,
        pos_tiering,
        real_pos_tiering,
        revenue_tiering
    from {{source('table_source', 'lp_pos_segmentation')}} 
    where month_year = to_char(dateadd(month, -1, to_date(concat('{{var("month_year")}}', '-01'))), 'yyyy-MM')
)

select * from data_previous_month
union all
select * from {{ref('pos_segmentation_calculation_temp')}}
