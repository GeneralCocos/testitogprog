CREATE SCHEMA IF NOT EXISTS raw;

CREATE TABLE IF NOT EXISTS raw.cbr_daily_xml (
    rate_date   date PRIMARY KEY,
    payload     xml NOT NULL,
    load_ts     timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.cbr_daily_rates (
    rate_date   date        NOT NULL,
    val_id      text        NOT NULL,
    num_code    text        NOT NULL,
    char_code   text        NOT NULL,
    nominal     integer     NOT NULL,
    name        text        NOT NULL,
    value       numeric(18,4) NOT NULL,
    load_ts     timestamptz DEFAULT now(),
    PRIMARY KEY (rate_date, char_code)
);
