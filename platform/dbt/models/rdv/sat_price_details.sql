{{ config(materialized='incremental') }}
{% set yaml_metadata %}
parent_hashkey: hk_price_h
src_hashdiff: hd_price_details_s
src_payload: [schema_version, effective_from, price_per_kwh]
source_model: stg_electricity_prices
{% endset %}
{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
