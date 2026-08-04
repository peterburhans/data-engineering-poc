{{ config(post_hook="{{ datavault4dbt.clean_up_pit('control_snap_v1') }}") }}

{% set yaml_metadata %}
tracked_entity: hub_price
hashkey: hk_price_h
sat_names:
  - sat_price_details
snapshot_relation: control_snap_v1
snapshot_trigger_column: is_latest
dimension_key: hk_price_d
ldts: ldts
sdts: sdts
refer_to_ghost_records: false
{% endset %}

with generated_pit as (
    {{ datavault4dbt.pit(yaml_metadata=yaml_metadata) }}
)
select *
from generated_pit
where hk_price_h is not null
