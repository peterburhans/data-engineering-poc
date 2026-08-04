{{ config(materialized='incremental') }}
{% set yaml_metadata %}
hashkey: hk_price_h
business_keys: [price_id]
source_models: stg_electricity_prices
disable_hwm: true
{% endset %}
{{ datavault4dbt.hub(yaml_metadata=yaml_metadata) }}
