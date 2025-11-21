CREATE SCHEMA IF NOT EXISTS dwh;

-- staging table where JDBC sink from Debezium lands raw cbr events
CREATE TABLE IF NOT EXISTS dwh.stg_cbr_daily_rates (
    event_ts   timestamptz DEFAULT now(),
    rate_date  date        NOT NULL,
    val_id     text        NOT NULL,
    num_code   text        NOT NULL,
    char_code  text        NOT NULL,
    nominal    integer     NOT NULL,
    name       text        NOT NULL,
    value      numeric(18,4) NOT NULL,
    PRIMARY KEY (rate_date, char_code)
);

-- cleansed layer used by DAG before shipping to Greenplum
CREATE TABLE IF NOT EXISTS dwh.ods_cbr_rates (
    rate_date  date        NOT NULL,
    char_code  text        NOT NULL,
    num_code   text        NOT NULL,
    val_id     text        NOT NULL,
    name       text        NOT NULL,
    nominal    integer     NOT NULL,
    value      numeric(18,4) NOT NULL,
    load_ts    timestamptz DEFAULT now(),
    PRIMARY KEY (rate_date, char_code)
);
