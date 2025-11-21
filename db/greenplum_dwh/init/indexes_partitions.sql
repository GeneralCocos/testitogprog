CREATE INDEX IF NOT EXISTS idx_fact_currency_rate_currency
    ON dwh.fact_currency_rate (currency_key);

CREATE INDEX IF NOT EXISTS idx_dim_date_month
    ON dwh.dim_date (month_start);

CREATE INDEX IF NOT EXISTS idx_alert_date
    ON dwh.currency_rate_alert (rate_date);
