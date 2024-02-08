{{
    config(
        post_hook = [
            "{{ delete_row('public.lp_pos_segmentation') }}",
            "{{ insert_row('public.lp_pos_segmentation', this) }}"
        ]
    )
}}

select 
    a.month_year,
    a.month_year_date,
    a.pos_code,
    a.revenue,
    a.pos_aging,
    a.pos_tiering,
    a.real_pos_tiering,
    a.revenue_tiering,
    a.previous_tiering,
    a.previous_real_tiering,
    a.previous_revenue,
    a.tiering_changes,
    a.etl_date,
    b.is_diamond,
    case 
        when b.is_diamond = 'Diamond' then 'Diamond' 
        when a.revenue_tiering = 'Diamond' then 'Platinum'
        else a.revenue_tiering 
    end new_tiering
from {{ref('pos_segmentation_stg_final')}} a
left join {{ref('pos_segmentation_check_diamond')}} b on a.pos_code = b.pos_code and 
concat(date_part(year, month_year_date), '-', date_part(quarter, month_year_date)) = quarter_next
where month_year = '{{var("month_year")}}'