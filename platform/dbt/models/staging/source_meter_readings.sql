select
    event_id,
    schema_version,
    meter_id,
    us_region,
    state_code,
    event_time,
    energy_kwh,
    voltage_v,
    current_a,
    power_factor,
    bucket_name,
    object_key,
    source_line_number,
    record_source,
    load_datetime
from {{ source('core_raw', 'smart_meter_events') }}
