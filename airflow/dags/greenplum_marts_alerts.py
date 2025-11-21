from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

GREENPLUM_CONN_ID = "greenplum_dwh"
ALERT_THRESHOLD = 0.05  # 5% day-over-day change


with DAG(
    dag_id="greenplum_marts_alerts",
    description="Строит витрины топ-5 валют и записывает алерты по резким скачкам",
    start_date=datetime(2025, 5, 20),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["greenplum", "datamart", "alerts"],
) as dag:

    @task
    def refresh_top5_mart() -> None:
        gp = PostgresHook(postgres_conn_id=GREENPLUM_CONN_ID)
        sql = """
        WITH fact AS (
            SELECT dd.month_start, dc.char_code, AVG(f.rate_value) AS avg_rate
            FROM dwh.fact_currency_rate f
            JOIN dwh.dim_date dd ON dd.date_key = f.rate_date_key
            JOIN dwh.dim_currency dc ON dc.currency_key = f.currency_key
            WHERE dd.full_date >= date_trunc('month', current_date) - interval '4 months'
            GROUP BY dd.month_start, dc.char_code
        ), ranked AS (
            SELECT month_start, char_code, avg_rate,
                   dense_rank() OVER (PARTITION BY month_start ORDER BY avg_rate DESC) AS rnk
            FROM fact
        )
        DELETE FROM datamart.top5_currency_rates
        WHERE month_start >= date_trunc('month', current_date) - interval '4 months';

        INSERT INTO datamart.top5_currency_rates (month_start, char_code, avg_rate, rank_in_month, refreshed_at)
        SELECT month_start, char_code, avg_rate, rnk, now()
        FROM ranked
        WHERE rnk <= 5;
        """
        gp.run(sql)

    @task
    def detect_rate_alerts() -> int:
        gp = PostgresHook(postgres_conn_id=GREENPLUM_CONN_ID)
        sql = """
        WITH prev_curr AS (
            SELECT f.rate_date_key, dc.char_code, f.rate_value,
                   LAG(f.rate_value) OVER (PARTITION BY dc.char_code ORDER BY f.rate_date_key) AS prev_rate
            FROM dwh.fact_currency_rate f
            JOIN dwh.dim_currency dc ON dc.currency_key = f.currency_key
        ),
        flagged AS (
            SELECT dd.full_date AS rate_date,
                   char_code,
                   prev_rate,
                   rate_value,
                   (rate_value - prev_rate) / NULLIF(prev_rate, 0) AS change_pct
            FROM prev_curr pc
            JOIN dwh.dim_date dd ON dd.date_key = pc.rate_date_key
            WHERE prev_rate IS NOT NULL
              AND (rate_value - prev_rate) / NULLIF(prev_rate, 0) >= %(threshold)s
        )
        INSERT INTO dwh.currency_rate_alert (rate_date, char_code, change_pct, prev_value, new_value)
        SELECT f.rate_date, f.char_code, f.change_pct, f.prev_rate, f.rate_value
        FROM flagged f
        WHERE NOT EXISTS (
            SELECT 1 FROM dwh.currency_rate_alert a
            WHERE a.rate_date = f.rate_date AND a.char_code = f.char_code
        );
        """
        gp.run(sql, parameters={"threshold": ALERT_THRESHOLD})
        result = gp.get_first("SELECT count(*) FROM dwh.currency_rate_alert")[0]
        return int(result)

    refresh_top5_mart() >> detect_rate_alerts()
