select
    a.month_year,
    a.month_year_date,
    a.pos_code,
    a.revenue,
    b.pos_aging,
    a.pos_tiering,
    a.real_pos_tiering,
    a.revenue_tiering
from (
    select
        '{{var("month_year")}}' as month_year,
        to_date(concat('{{var("month_year")}}', '-01')) as month_year_date,
        pos_code,
        0 as revenue,
        'No Trx' as pos_tiering,
        'Undefined' as real_pos_tiering,
        'Undefined' as revenue_tiering
    from (
        select
            month_year,
            pos_code,
            revenue,
            pos_tiering,
            row_number() over (partition by pos_code order by month_year_date desc) as rownum
        from {{ref('pos_segmentation_stg')}}
    ) where rownum = 1 and month_year <> '{{var("month_year")}}'   
) a
left join {{ref('pos_segmentation_pos_aging_booked_temp')}} b on a.pos_code = b.pos_code