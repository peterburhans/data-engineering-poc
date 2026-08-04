{{ config(materialized='incremental') }}
{% set yaml_metadata %}
parent_hashkey: hk_meter_h
src_hashdiff: hd_meter_details_s
src_payload: [schema_version, us_region, state_code]
source_model: stg_meter_readings
{% endset %}
{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
