select
    md5(meter_id) as meter_key,
    to_char(date_trunc('hour', event_time), 'YYYYMMDD')::integer as date_key,
    extract(hour from event_time)::integer as time_key,
    md5(state_code) as pricing_zone_key,
    date_trunc('hour', event_time) as reading_hour,
    sum(energy_kwh) as energy_kwh,
    round((sum(usage_revenue) / nullif(sum(energy_kwh), 0))::numeric, 6) as price_per_kwh,
    sum(usage_revenue) as usage_revenue,
    count(*) as reading_count
from {{ ref('int_meter_readings_priced') }}
group by
    meter_id,
    state_code,
    date_trunc('hour', event_time),
    to_char(date_trunc('hour', event_time), 'YYYYMMDD')::integer,
    extract(hour from event_time)::integer
