{{ config(materialized='incremental') }}
{% set yaml_metadata %}
link_hashkey: hk_meter_reading_l
foreign_hashkeys: [hk_meter_h, hk_reading_h]
source_models: stg_meter_readings
disable_hwm: true
{% endset %}
{{ datavault4dbt.link(yaml_metadata=yaml_metadata) }}
