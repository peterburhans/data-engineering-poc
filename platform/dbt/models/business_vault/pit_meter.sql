{{ config(post_hook="{{ datavault4dbt.clean_up_pit('control_snap_v1') }}") }}

{% set yaml_metadata %}
tracked_entity: hub_meter
hashkey: hk_meter_h
sat_names:
  - sat_meter_details
snapshot_relation: control_snap_v1
snapshot_trigger_column: is_active
dimension_key: hk_meter_d
ldts: ldts
sdts: sdts
refer_to_ghost_records: false
{% endset %}

with generated_pit as (
    {{ datavault4dbt.pit(yaml_metadata=yaml_metadata) }}
)
select *
from generated_pit
where hk_meter_h is not null
