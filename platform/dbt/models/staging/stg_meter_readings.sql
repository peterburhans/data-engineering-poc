{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: source_meter_readings
ldts: load_datetime
rsrc: record_source
enable_ghost_records: false
hashed_columns:
  hk_meter_h:
    - meter_id
  hk_reading_h:
    - event_id
  hk_pricing_zone_h:
    - state_code
  hk_meter_reading_l:
    - meter_id
    - event_id
  hk_meter_pricing_zone_l:
    - meter_id
    - state_code
  hd_meter_details_s:
    is_hashdiff: true
    columns:
      - schema_version
      - us_region
      - state_code
  hd_reading_metrics_s:
    is_hashdiff: true
    columns:
      - event_time
      - energy_kwh
      - voltage_v
      - current_a
      - power_factor
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
