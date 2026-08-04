select * from {{ source('core_raw', 'electricity_prices') }}
