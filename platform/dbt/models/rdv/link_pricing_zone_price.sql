{{ config(materialized='incremental') }}
{% set yaml_metadata %}
link_hashkey: hk_pricing_zone_price_l
foreign_hashkeys: [hk_pricing_zone_h, hk_price_h]
source_models: stg_electricity_prices
disable_hwm: true
{% endset %}
{{ datavault4dbt.link(yaml_metadata=yaml_metadata) }}
