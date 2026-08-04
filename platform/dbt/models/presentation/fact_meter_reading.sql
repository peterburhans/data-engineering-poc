select
    md5(event_id::text) as reading_key,
    event_id,
    md5(meter_id) as meter_key,
    to_char(event_time, 'YYYYMMDD')::integer as date_key,
    extract(hour from event_time)::integer as time_key,
    md5(state_code) as pricing_zone_key,
    event_time,
    energy_kwh,
    voltage_v,
    current_a,
    power_factor,
    load_datetime
from {{ ref('int_meter_readings') }}
