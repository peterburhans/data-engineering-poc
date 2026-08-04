with hours as (
    select distinct extract(hour from date_hour)::integer as hour_of_day
    from {{ ref('time_spine_hourly') }}
)

select
    hour_of_day as time_key,
    make_time(hour_of_day, 0, 0) as hour_start,
    lpad(hour_of_day::text, 2, '0') || ':00' as hour_label,
    case
        when hour_of_day between 0 and 5 then 'Overnight'
        when hour_of_day between 6 and 11 then 'Morning'
        when hour_of_day between 12 and 17 then 'Afternoon'
        else 'Evening'
    end as day_part
from hours
