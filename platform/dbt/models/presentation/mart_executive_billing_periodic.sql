{% set grains = [
    ('Year', 1, "date_trunc('year', reading_hour)"),
    ('Month', 2, "date_trunc('month', reading_hour)"),
    ('Week', 3, "date_trunc('week', reading_hour)"),
    ('Day', 4, "date_trunc('day', reading_hour)"),
    ('Hour', 5, "date_trunc('hour', reading_hour)")
] %}

with data_bounds as (
    select min(reading_hour) as data_start
    from {{ ref('fact_hourly_energy_billing') }}
)

{% for grain, grain_sort_order, period_expression in grains %}
select
    '{{ grain }}'::text as period_grain,
    {{ grain_sort_order }}::smallint as period_grain_order,
    greatest({{ period_expression }}, b.data_start) as period_start,
    m.meter_id,
    m.us_region,
    m.state_code,
    z.currency_code,
    sum(f.energy_kwh) as energy_kwh,
    sum(f.usage_revenue) as net_revenue,
    round((sum(f.usage_revenue) * 0.05)::numeric, 4) as tax_amount,
    round((sum(f.usage_revenue) * 1.05)::numeric, 4) as gross_billed,
    round((sum(f.usage_revenue) / nullif(sum(f.energy_kwh), 0))::numeric, 4)
        as realized_price_per_kwh,
    sum(f.usage_revenue) as revenue_per_meter
from {{ ref('fact_hourly_energy_billing') }} f
join {{ ref('dim_meter') }} m using (meter_key)
join {{ ref('dim_pricing_zone') }} z using (pricing_zone_key)
cross join data_bounds b
{{ dbt_utils.group_by(n=7) }}
{% if not loop.last %}
union all
{% endif %}
{% endfor %}
