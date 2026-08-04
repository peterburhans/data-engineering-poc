{{
    dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2036-01-01' as date)"
    )
}}
