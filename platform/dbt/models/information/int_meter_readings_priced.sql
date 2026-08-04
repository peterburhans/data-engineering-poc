{{ config(materialized='table') }}

select
    r.event_id,
    r.meter_id,
    r.us_region,
    r.state_code,
    r.event_time,
    r.energy_kwh,
    r.voltage_v,
    r.current_a,
    r.power_factor,
    r.schema_version,
    p.price_id,
    p.currency_code,
    p.effective_from as price_effective_from,
    p.price_per_kwh,
    round((r.energy_kwh * p.price_per_kwh)::numeric, 4) as usage_revenue,
    greatest(r.load_datetime, p.load_datetime) as load_datetime
from {{ ref('int_meter_readings') }} r
join {{ ref('int_electricity_prices') }} p
  on p.state_code = r.state_code
 and p.us_region = r.us_region
 and r.event_time >= p.effective_from
 and (r.event_time < p.effective_to or p.effective_to is null)
