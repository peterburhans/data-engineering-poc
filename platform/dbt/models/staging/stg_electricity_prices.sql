{{ config(materialized='view') }}

{%- set yaml_metadata -%}
source_model: source_electricity_prices
ldts: load_datetime
rsrc: source_system
enable_ghost_records: false
hashed_columns:
  hk_pricing_zone_h:
    - state_code
  hk_price_h:
    - price_id
  hk_pricing_zone_price_l:
    - state_code
    - price_id
  hd_price_details_s:
    is_hashdiff: true
    columns:
      - schema_version
      - effective_from
      - price_per_kwh
  hd_pricing_zone_details_s:
    is_hashdiff: true
    columns:
      - us_region
      - currency_code
{%- endset -%}

{{ datavault4dbt.stage(yaml_metadata=yaml_metadata) }}
