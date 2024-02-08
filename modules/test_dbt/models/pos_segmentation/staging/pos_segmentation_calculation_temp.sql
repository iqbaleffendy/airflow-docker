select
    a.month_year,
    a.month_year_date,
    a.pos_code,
    a.revenue,
    b.pos_aging,
    case 
        when b.pos_aging < 3 then 'NEW'
        when revenue > 0 and revenue < 10000000 then 'Regular'
        when revenue >= 10000000 and revenue < 25000000 then 'Regular'
        when revenue >= 25000000 and revenue < 50000000 then 'G/P'
        when revenue >= 50000000 and revenue < 150000000 then 'G/P'
        when revenue >= 150000000 then 'G/P'
        else 'Undefined'
    end as pos_tiering,
    case 
        when revenue > 0 and revenue < 10000000 then 'Regular'
        when revenue >= 10000000 and revenue < 25000000 then 'Regular'
        when revenue >= 25000000 and revenue < 50000000 then 'G/P'
        when revenue >= 50000000 and revenue < 150000000 then 'G/P'
        when revenue >= 150000000 then 'G/P'
        else 'Undefined'
    end as real_pos_tiering,
    case 
        when revenue > 0 and revenue < 10000000 then 'Bronze'
        when revenue >= 10000000 and revenue < 25000000 then 'Silver'
        when revenue >= 25000000 and revenue < 50000000 then 'Gold'
        when revenue >= 50000000 and revenue < 150000000 then 'Platinum'
        when revenue >= 150000000 then 'Diamond'
        else 'Undefined'
    end as revenue_tiering
from {{ref('pos_segmentation_raw_temp')}} a
left join {{ref('pos_segmentation_pos_aging_booked_temp')}} b on a.pos_code = b.pos_code