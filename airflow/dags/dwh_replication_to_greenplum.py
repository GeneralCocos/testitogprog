from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Tuple

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DWH_CONN_ID = "dwh_postgres"
GREENPLUM_CONN_ID = "greenplum_dwh"


def _prepare_dim_date_rows(records: List[Tuple]) -> List[Tuple]:
    rows: List[Tuple] = []
    for rate_date, *_ in records:
        date_key = int(rate_date.strftime("%Y%m%d"))
        month_start = rate_date.replace(day=1)
        rows.append(
            (
                date_key,
                rate_date,
                month_start,
                rate_date.year,
                rate_date.month,
            )
        )
    return rows


def _prepare_dim_currency_rows(records: List[Tuple]) -> List[Tuple]:
    seen = set()
    rows: List[Tuple] = []
    for _, val_id, num_code, char_code, nominal, name, _ in records:
        if char_code in seen:
            continue
        seen.add(char_code)
        rows.append((char_code, num_code, val_id, name, nominal))
    return rows


def _prepare_fact_rows(records: List[Tuple]) -> List[Tuple]:
    rows: List[Tuple] = []
    for rate_date, _, _, char_code, _, _, value in records:
        date_key = int(rate_date.strftime("%Y%m%d"))
        rows.append((date_key, char_code, value))
    return rows


with DAG(
    dag_id="dwh_replication_to_greenplum",
    description="Собирает слой ODS в Postgres и выгружает в Greenplum по модели Кимбола",
    start_date=datetime(2025, 5, 20),
    schedule_interval="@daily",
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["cbr", "dwh", "greenplum"],
) as dag:

    @task
    def upsert_ods_from_stage() -> int:
        dwh = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        sql = """
            INSERT INTO dwh.ods_cbr_rates AS tgt
                (rate_date, char_code, num_code, val_id, name, nominal, value)
            SELECT rate_date, char_code, num_code, val_id, name, nominal, value
            FROM dwh.stg_cbr_daily_rates
            ON CONFLICT (rate_date, char_code) DO UPDATE
              SET num_code = EXCLUDED.num_code,
                  val_id   = EXCLUDED.val_id,
                  name     = EXCLUDED.name,
                  nominal  = EXCLUDED.nominal,
                  value    = EXCLUDED.value,
                  load_ts  = now();
        """
        dwh.run(sql)
        row_count = dwh.get_first("SELECT count(*) FROM dwh.ods_cbr_rates")[0]
        return int(row_count)

    @task
    def replicate_to_greenplum() -> int:
        dwh = PostgresHook(postgres_conn_id=DWH_CONN_ID)
        gp = PostgresHook(postgres_conn_id=GREENPLUM_CONN_ID)

        records = dwh.get_records(
            "SELECT rate_date, val_id, num_code, char_code, nominal, name, value"
            " FROM dwh.ods_cbr_rates"
        )
        if not records:
            return 0

        dim_date_rows = _prepare_dim_date_rows(records)
        dim_cur_rows = _prepare_dim_currency_rows(records)
        fact_rows = _prepare_fact_rows(records)

        for date_key, full_date, month_start, year_num, month_num in dim_date_rows:
            gp.run(
                """
                INSERT INTO dwh.dim_date (date_key, full_date, month_start, year_num, month_num)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (date_key) DO UPDATE
                  SET full_date = EXCLUDED.full_date,
                      month_start = EXCLUDED.month_start,
                      year_num = EXCLUDED.year_num,
                      month_num = EXCLUDED.month_num;
                """,
                parameters=(date_key, full_date, month_start, year_num, month_num),
            )

        for char_code, num_code, val_id, name, nominal in dim_cur_rows:
            gp.run(
                """
                INSERT INTO dwh.dim_currency (char_code, num_code, val_id, name, nominal)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (char_code) DO UPDATE
                  SET num_code = EXCLUDED.num_code,
                      val_id   = EXCLUDED.val_id,
                      name     = EXCLUDED.name,
                      nominal  = EXCLUDED.nominal,
                      effective_from = LEAST(dim_currency.effective_from, current_date);
                """,
                parameters=(char_code, num_code, val_id, name, nominal),
            )

        for date_key, char_code, rate_value in fact_rows:
            gp.run(
                """
                INSERT INTO dwh.fact_currency_rate AS f (rate_date_key, currency_key, rate_value)
                SELECT dd.date_key, dc.currency_key, %s
                FROM dwh.dim_date dd
                JOIN dwh.dim_currency dc ON dc.char_code = %s
                WHERE dd.date_key = %s
                ON CONFLICT (rate_date_key, currency_key) DO UPDATE
                  SET rate_value = EXCLUDED.rate_value,
                      load_ts = now();
                """,
                parameters=(rate_value, char_code, date_key),
            )

        return len(fact_rows)

    upsert_ods_from_stage() >> replicate_to_greenplum()
