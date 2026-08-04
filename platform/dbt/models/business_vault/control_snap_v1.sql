{{ config(materialized='view') }}

{% set yaml_metadata %}
control_snap_v0: 'control_snap_v0'
log_logic:
  daily:
    duration: 30
    unit: 'DAY'
  weekly:
    duration: 6
    unit: 'MONTH'
  monthly:
    duration: 3
    unit: 'YEAR'
  yearly:
    forever: true
{% endset %}

{{ datavault4dbt.control_snap_v1(yaml_metadata=yaml_metadata) }}
