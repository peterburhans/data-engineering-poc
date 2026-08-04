select date_hour
from (
    {{
        dbt_utils.date_spine(
            datepart="hour",
            start_date="cast('2020-01-01 00:00:00' as timestamp)",
            end_date="cast('2036-01-01 00:00:00' as timestamp)"
        )
    }}
) as hourly_spine(date_hour)
