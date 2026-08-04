select
    md5(meter_id) as meter_key,
    meter_id,
    us_region,
    state_code,
    min(event_time) as first_reading_at,
    max(event_time) as latest_reading_at
from {{ ref('int_meter_readings') }}
group by meter_id, us_region, state_code
