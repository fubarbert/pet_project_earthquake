import logging

import duckdb
import pendulum
import psycopg2
from psycopg2.extras import execute_values
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

OWNER = "fubarbert"
DAG_ID = "raw_from_s3_to_pg"

LAYER = "raw"
SOURCE = "earthquake"
SCHEMA = "ods"
TARGET_TABLE = "fct_earthquake"

LONG_DESCRIPTION = """# LONG DESCRIPTION"""
SHORT_DESCRIPTION = "SHORT DESCRIPTION"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 5, 1, tz="Europe/Moscow"),
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[str, str]:
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date


def create_table_if_not_exists(**context):
    password = Variable.get("pg_password")
    conn = psycopg2.connect(
        host="postgres_dwh", port=5432,
        database="postgres", user="postgres", password=password
    )
    try:
        cur = conn.cursor()
        cur.execute("CREATE SCHEMA IF NOT EXISTS ods")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ods.fct_earthquake (
                time             TIMESTAMP,
                latitude         FLOAT,
                longitude        FLOAT,
                depth            FLOAT,
                mag              FLOAT,
                mag_type         VARCHAR,
                nst              INTEGER,
                gap              FLOAT,
                dmin             FLOAT,
                rms              FLOAT,
                net              VARCHAR,
                id               VARCHAR,
                updated          TIMESTAMP,
                place            VARCHAR,
                type             VARCHAR,
                horizontal_error FLOAT,
                depth_error      FLOAT,
                mag_error        FLOAT,
                mag_nst          FLOAT,
                status           VARCHAR,
                location_source  VARCHAR,
                mag_source       VARCHAR
            )
        """)
        conn.commit()
        cur.close()
    finally:
        conn.close()
    logging.info("Schema and table created (if not existed)")


def fetch_and_transfer_raw_data_to_ods_pg(**context):
    access_key = Variable.get("access_key").replace("'", "''")
    secret_key = Variable.get("secret_key").replace("'", "''")
    password = Variable.get("pg_password")

    start_date, end_date = get_dates(**context)
    logging.info(f"Start load for dates: {start_date}/{end_date}")

    # Читаем из S3 через DuckDB в pandas DataFrame
    con = duckdb.connect()
    try:
        con.sql("INSTALL httpfs")
        con.sql("LOAD httpfs")
        con.sql("SET s3_url_style = 'path'")
        con.sql("SET s3_endpoint = 'minio:9000'")
        con.sql(f"SET s3_access_key_id = '{access_key}'")
        con.sql(f"SET s3_secret_access_key = '{secret_key}'")
        con.sql("SET s3_use_ssl = FALSE")
        df = con.sql(f"""
            SELECT
                time, latitude, longitude, depth, mag,
                magType          AS mag_type,
                nst, gap, dmin, rms, net, id, updated, place, type,
                horizontalError  AS horizontal_error,
                depthError       AS depth_error,
                magError         AS mag_error,
                magNst           AS mag_nst,
                status,
                locationSource   AS location_source,
                magSource        AS mag_source
            FROM 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet'
        """).df()
    finally:
        con.close()

    logging.info(f"Rows fetched from S3: {len(df)}")

    # Пишем в PostgreSQL через psycopg2
    conn = psycopg2.connect(
        host="postgres_dwh", port=5432,
        database="postgres", user="postgres", password=password
    )
    try:
        cur = conn.cursor()
        rows = [tuple(None if str(v) == 'nan' else v.item() if hasattr(v, 'item') else v for v in row) for row in df.itertuples(index=False)]
        execute_values(cur, f"""
            INSERT INTO {SCHEMA}.{TARGET_TABLE} (
                time, latitude, longitude, depth, mag, mag_type,
                nst, gap, dmin, rms, net, id, updated, place, type,
                horizontal_error, depth_error, mag_error, mag_nst,
                status, location_source, mag_source
            ) VALUES %s
        """, rows)
        conn.commit()
        cur.close()
    finally:
        conn.close()

    logging.info(f"Load for date success: {start_date}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    catchup=True,
    tags=["s3", "ods", "pg"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(task_id="start")

    sensor_on_raw_layer = ExternalTaskSensor(
        task_id="sensor_on_raw_layer",
        external_dag_id="raw_from_api_to_s3",
        external_task_id="end",
        allowed_states=["success"],
        mode="reschedule",
        timeout=360000,
        poke_interval=60,
    )

    create_table_task = PythonOperator(
        task_id="create_table_if_not_exists",
        python_callable=create_table_if_not_exists,
    )

    transfer_raw_data_task = PythonOperator(
        task_id="get_and_transfer_raw_data_to_ods_pg",
        python_callable=fetch_and_transfer_raw_data_to_ods_pg,
    )

    end = EmptyOperator(task_id="end")

    start >> sensor_on_raw_layer >> create_table_task >> transfer_raw_data_task >> end