select
    to_char(date_day, 'YYYYMMDD')::integer as date_key,
    date_day::date as calendar_date,
    extract(year from date_day)::integer as calendar_year,
    extract(quarter from date_day)::integer as calendar_quarter,
    extract(month from date_day)::integer as calendar_month,
    trim(to_char(date_day, 'Month')) as month_name,
    extract(isodow from date_day)::integer as iso_day_of_week,
    extract(isodow from date_day) in (6, 7) as is_weekend
from {{ ref('time_spine_daily') }}
