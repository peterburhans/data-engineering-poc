with ghost_records as (
    select 'hub_meter' as model_name, hk_meter_h as hash_key
    from {{ ref('hub_meter') }}
    where rsrc in ('SYSTEM', 'ERROR')

    union all

    select 'hub_reading', hk_reading_h
    from {{ ref('hub_reading') }}
    where rsrc in ('SYSTEM', 'ERROR')

    union all

    select 'link_meter_reading', hk_meter_reading_l
    from {{ ref('link_meter_reading') }}
    where rsrc in ('SYSTEM', 'ERROR')

    union all

    select 'sat_meter_details', hk_meter_h
    from {{ ref('sat_meter_details') }}
    where rsrc in ('SYSTEM', 'ERROR')

    union all

    select 'sat_reading_metrics', hk_reading_h
    from {{ ref('sat_reading_metrics') }}
    where rsrc in ('SYSTEM', 'ERROR')
)

select *
from ghost_records
