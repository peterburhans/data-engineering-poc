select
    md5(state_code) as pricing_zone_key,
    min(us_region) as us_region,
    state_code,
    min(currency_code) as currency_code
from {{ ref('int_electricity_prices') }}
group by state_code
