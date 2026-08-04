-- Operational marts need same-day state. PIT models remain the supported path for
-- closed-period/as-of reporting, while these CTEs expose the latest loaded state.
with current_meter_details as (
    select distinct on (hk_meter_h) *
    from {{ ref('sat_meter_details') }}
    order by hk_meter_h, ldts desc
), current_reading_metrics as (
    select distinct on (hk_reading_h) *
    from {{ ref('sat_reading_metrics') }}
    order by hk_reading_h, ldts desc
)

select
    r.event_id,
    m.meter_id,
    md.us_region,
    md.state_code,
    rm.event_time,
    rm.energy_kwh,
    rm.voltage_v,
    rm.current_a,
    rm.power_factor,
    md.schema_version,
    rm.ldts as load_datetime
from {{ ref('link_meter_reading') }} l
join {{ ref('hub_meter') }} m using (hk_meter_h)
join {{ ref('hub_reading') }} r using (hk_reading_h)
join current_meter_details md using (hk_meter_h)
join current_reading_metrics rm using (hk_reading_h)
where r.event_id is not null
