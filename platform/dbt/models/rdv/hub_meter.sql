{{ config(materialized='incremental') }}
{% set yaml_metadata %}
hashkey: hk_meter_h
business_keys: [meter_id]
source_models: stg_meter_readings
disable_hwm: true
{% endset %}
{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}
