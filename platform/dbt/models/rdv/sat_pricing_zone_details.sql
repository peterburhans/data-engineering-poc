{{ config(materialized='incremental') }}
{% set yaml_metadata %}
parent_hashkey: hk_pricing_zone_h
src_hashdiff: hd_pricing_zone_details_s
src_payload: [us_region, currency_code]
source_model: stg_electricity_prices
{% endset %}
{{ datavault4dbt.sat_v0(yaml_metadata=yaml_metadata) }}
