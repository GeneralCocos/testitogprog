CREATE SCHEMA IF NOT EXISTS dwh;
CREATE SCHEMA IF NOT EXISTS datamart;

-- Kimball-style dimensions
CREATE TABLE IF NOT EXISTS dwh.dim_currency (
    currency_key bigserial PRIMARY KEY,
    char_code    text UNIQUE NOT NULL,
    num_code     text NOT NULL,
    val_id       text NOT NULL,
    name         text NOT NULL,
    nominal      integer NOT NULL,
    effective_from date DEFAULT current_date,
    effective_to   date DEFAULT '9999-12-31'
);

CREATE TABLE IF NOT EXISTS dwh.dim_date (
    date_key    integer PRIMARY KEY,
    full_date   date NOT NULL UNIQUE,
    month_start date NOT NULL,
    year_num    integer NOT NULL,
    month_num   integer NOT NULL
);

-- Fact table with only needed grain (daily value per currency)
CREATE TABLE IF NOT EXISTS dwh.fact_currency_rate (
    rate_date_key integer NOT NULL REFERENCES dwh.dim_date(date_key),
    currency_key  bigint  NOT NULL REFERENCES dwh.dim_currency(currency_key),
    rate_value    numeric(18,4) NOT NULL,
    load_ts       timestamptz DEFAULT now(),
    PRIMARY KEY (rate_date_key, currency_key)
)
PARTITION BY RANGE (rate_date_key)
(
    START (20200101) END (20300101) EVERY (1)
)
DISTRIBUTED BY (currency_key);

-- Alert storage
CREATE TABLE IF NOT EXISTS dwh.currency_rate_alert (
    alert_id    bigserial PRIMARY KEY,
    rate_date   date NOT NULL,
    char_code   text NOT NULL,
    change_pct  numeric(12,4) NOT NULL,
    prev_value  numeric(18,4) NOT NULL,
    new_value   numeric(18,4) NOT NULL,
    created_at  timestamptz DEFAULT now()
);

-- Data mart storing top 5 currencies by average value per month
CREATE TABLE IF NOT EXISTS datamart.top5_currency_rates (
    month_start date NOT NULL,
    char_code   text NOT NULL,
    avg_rate    numeric(18,4) NOT NULL,
    rank_in_month integer NOT NULL,
    refreshed_at timestamptz DEFAULT now(),
    PRIMARY KEY (month_start, char_code)
);
