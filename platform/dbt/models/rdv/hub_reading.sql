{{ config(materialized='incremental') }}
{% set yaml_metadata %}
hashkey: hk_reading_h
business_keys: [event_id]
source_models: stg_meter_readings
disable_hwm: true
{% endset %}
{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}
