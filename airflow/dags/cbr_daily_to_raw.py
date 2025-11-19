from __future__ import annotations

from datetime import datetime, timedelta
from typing import List, Dict

import requests
import xml.etree.ElementTree as ET

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook


RAW_CONN_ID = "raw_postgres"
CBR_BASE_URL = "https://www.cbr.ru"


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


def _fetch_cbr_daily_xml(date_obj: datetime) -> bytes:
    date_str = date_obj.strftime("%d/%m/%Y")
    url = f"{CBR_BASE_URL}/scripts/XML_daily.asp"
    params = {"date_req": date_str}

    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.content


def _parse_cbr_daily(xml_bytes: bytes, date_obj: datetime) -> List[Dict]:
    root = ET.fromstring(xml_bytes)
    rows: List[Dict] = []
    rate_date = date_obj.date()

    for valute_el in root.findall("Valute"):
        val_id = valute_el.get("ID")
        num_code = valute_el.findtext("NumCode")
        char_code = valute_el.findtext("CharCode")
        nominal_text = valute_el.findtext("Nominal")
        name = valute_el.findtext("Name")
        value_text = valute_el.findtext("Value")

        try:
            nominal = int(nominal_text) if nominal_text else 1
        except ValueError:
            nominal = 1

        if value_text:
            value_num = float(value_text.replace(",", "."))
        else:
            value_num = 0.0

        rows.append(
            {
                "rate_date": rate_date,
                "val_id": val_id,
                "num_code": num_code,
                "char_code": char_code,
                "nominal": nominal,
                "name": name,
                "value": value_num,
            }
        )

    return rows



with DAG(
        dag_id="cbr_daily_to_raw",
        description="Загрузка ежедневных курсов валют ЦБ РФ в RAW Postgres",
        start_date=datetime(2025, 5, 18),
        schedule_interval="@daily",
        catchup=True,
        default_args=default_args,
        tags=["cbr", "raw", "currency"],
) as dag:

    @task
    def load_cbr_daily_for_date(logical_date: datetime, **context) -> int:
        pg = PostgresHook(postgres_conn_id=RAW_CONN_ID)
        date_obj = logical_date

        xml_bytes = _fetch_cbr_daily_xml(date_obj)
        xml_text = xml_bytes.decode("windows-1251", errors="ignore")

        pg.run(
            """
            INSERT INTO raw.cbr_daily_xml (rate_date, payload)
            VALUES (%s, %s::xml)
            ON CONFLICT (rate_date) DO UPDATE
              SET payload = EXCLUDED.payload,
                  load_ts = now();
            """,
            parameters=(date_obj.date(), xml_text),
        )

        rows = _parse_cbr_daily(xml_bytes, date_obj)
        if not rows:
            return 0

        sql_rates = """
            INSERT INTO raw.cbr_daily_rates
                (rate_date, val_id, num_code, char_code, nominal, name, value)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (rate_date, char_code) DO UPDATE
              SET val_id  = EXCLUDED.val_id,
                  num_code= EXCLUDED.num_code,
                  nominal= EXCLUDED.nominal,
                  name    = EXCLUDED.name,
                  value   = EXCLUDED.value,
                  load_ts = now();
        """

        for r in rows:
            pg.run(
                sql_rates,
                parameters=(
                    r["rate_date"],
                    r["val_id"],
                    r["num_code"],
                    r["char_code"],
                    r["nominal"],
                    r["name"],
                    r["value"],
                ),
            )

        return len(rows)

    load_cbr_daily_for_date()
