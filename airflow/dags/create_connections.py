from airflow import settings
from airflow.models import Connection


def upsert_connection(conn: Connection):
    session = settings.Session()
    try:
        existing = (
            session.query(Connection)
            .filter(Connection.conn_id == conn.conn_id)
            .one_or_none()
        )
        if existing:
            existing.conn_type = conn.conn_type
            existing.host = conn.host
            existing.schema = conn.schema
            existing.login = conn.login
            existing.password = conn.password
            existing.port = conn.port
            existing.extra = conn.extra
            session.add(existing)
        else:
            session.add(conn)
        session.commit()
        print(f"[OK] Connection {conn.conn_id} created/updated")
    except Exception as e:
        session.rollback()
        print(f"[ERR] {e}")
        raise
    finally:
        session.close()


def create_raw_postgres_connection():
    conn = Connection(
        conn_id="raw_postgres",
        conn_type="postgres",
        host="postgres-raw",
        schema="airflow",   # как в POSTGRES_DB
        login="airflow",
        password="airflow",
        port=5432,
    )
    upsert_connection(conn)


def main():
    create_raw_postgres_connection()
