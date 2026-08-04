{{ config(materialized='incremental') }}
{% set yaml_metadata %}
parent_hashkey: hk_reading_h
src_hashdiff: hd_reading_metrics_s
src_payload: [event_time, energy_kwh, voltage_v, current_a, power_factor]
source_model: stg_meter_readings
{% endset %}
{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
