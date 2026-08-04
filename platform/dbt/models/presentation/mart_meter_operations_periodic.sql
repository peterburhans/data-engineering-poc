{% set grains = [
    ('Year', 1, "date_trunc('year', f.event_time)"),
    ('Month', 2, "date_trunc('month', f.event_time)"),
    ('Week', 3, "date_trunc('week', f.event_time)"),
    ('Day', 4, "date_trunc('day', f.event_time)"),
    ('Hour', 5, "date_trunc('hour', f.event_time)")
] %}

with data_bounds as (
    select min(event_time) as data_start
    from {{ ref('fact_meter_reading') }}
)

{% for grain, grain_sort_order, period_expression in grains %}
select
    '{{ grain }}'::text as period_grain,
    {{ grain_sort_order }}::smallint as period_grain_order,
    greatest({{ period_expression }}, b.data_start) as period_start,
    m.meter_id,
    m.us_region,
    m.state_code,
    sum(f.energy_kwh) as energy_kwh,
    sum(f.voltage_v) as voltage_sum,
    sum(f.power_factor) as power_factor_sum,
    count(*) as reading_count
from {{ ref('fact_meter_reading') }} f
join {{ ref('dim_meter') }} m using (meter_key)
cross join data_bounds b
{{ dbt_utils.group_by(n=6) }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
