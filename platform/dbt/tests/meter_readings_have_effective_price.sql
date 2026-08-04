select
    r.event_id,
    r.state_code,
    r.event_time
from {{ ref('int_meter_readings') }} r
left join {{ ref('int_meter_readings_priced') }} priced
  on priced.event_id = r.event_id
where priced.event_id is null
