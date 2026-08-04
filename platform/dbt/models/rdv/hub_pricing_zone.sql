{{ config(materialized='incremental') }}
{% set yaml_metadata %}
hashkey: hk_pricing_zone_h
business_keys: [state_code]
source_models: stg_electricity_prices
disable_hwm: true
{% endset %}
{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}
