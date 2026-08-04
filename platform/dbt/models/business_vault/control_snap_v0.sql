{{ config(materialized='incremental') }}

{% set yaml_metadata %}
start_date: '2020-01-01'
daily_snapshot_time: '06:00:00'
{% endset %}

{{ datavault4dbt.control_snap_v0(yaml_metadata=yaml_metadata) }}
