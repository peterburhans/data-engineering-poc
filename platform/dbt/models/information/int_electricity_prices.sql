-- Billing needs newly loaded intraday prices. Retained PIT snapshots serve
-- closed-period/as-of reporting and intentionally do not include today's loads.
with current_price_details as (
    select distinct on (hk_price_h) *
    from {{ ref('sat_price_details') }}
    order by hk_price_h, ldts desc
), current_pricing_zone_details as (
    select distinct on (hk_pricing_zone_h) *
    from {{ ref('sat_pricing_zone_details') }}
    order by hk_pricing_zone_h, ldts desc
), vault_prices as (
    select
        p.price_id,
        z.state_code,
        zd.us_region,
        zd.currency_code,
        pd.effective_from,
        lead(pd.effective_from) over (
            partition by z.state_code
            order by pd.effective_from
        ) as effective_to,
        pd.price_per_kwh,
        greatest(pd.ldts, zd.ldts) as load_datetime
    from {{ ref('link_pricing_zone_price') }} l
    join {{ ref('hub_pricing_zone') }} z using (hk_pricing_zone_h)
    join {{ ref('hub_price') }} p using (hk_price_h)
    join current_price_details pd using (hk_price_h)
    join current_pricing_zone_details zd using (hk_pricing_zone_h)
)
select * from vault_prices
