CREATE EXTENSION IF NOT EXISTS pg_mooncake CASCADE;
CREATE SCHEMA IF NOT EXISTS core_raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS rdv;
CREATE SCHEMA IF NOT EXISTS business_vault;
CREATE SCHEMA IF NOT EXISTS information;
CREATE SCHEMA IF NOT EXISTS presentation;
CREATE SCHEMA IF NOT EXISTS semantic;

CREATE TABLE IF NOT EXISTS core_raw.smart_meter_events (
    event_id uuid PRIMARY KEY,
    schema_version text NOT NULL,
    meter_id text NOT NULL,
    us_region text NOT NULL,
    state_code text NOT NULL,
    event_time timestamptz NOT NULL,
    energy_kwh numeric(12,5) NOT NULL,
    voltage_v numeric(8,2) NOT NULL,
    current_a numeric(8,3) NOT NULL,
    power_factor numeric(5,3) NOT NULL,
    bucket_name text NOT NULL,
    object_key text NOT NULL,
    source_line_number integer NOT NULL,
    record_source text NOT NULL,
    load_datetime timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core_raw.electricity_prices (
    price_id uuid PRIMARY KEY,
    schema_version text NOT NULL,
    us_region text NOT NULL,
    state_code text NOT NULL,
    currency_code char(3) NOT NULL,
    effective_from timestamptz NOT NULL,
    price_per_kwh numeric(10,4) NOT NULL CHECK (price_per_kwh >= 0),
    source_system text NOT NULL,
    load_datetime timestamptz NOT NULL DEFAULT now(),
    UNIQUE (state_code, effective_from)
);
