from __future__ import annotations

import json
import uuid
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

RAW_JSONL_PATH = "/opt/airflow/data/raw/events.jsonl"

DEFAULT_ARGS = {"owner": "you", "retries": 0}

def load_jsonl_to_raw(**context):
    load_id = str(uuid.uuid4())
    context["ti"].xcom_push(key="load_id", value=load_id)

    hook = PostgresHook(postgres_conn_id="warehouse_pg")
    conn = hook.get_conn()
    cur = conn.cursor()

    line_no = 0
    batch = []
    batch_size = 2000

    with open(RAW_JSONL_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            line_no += 1
            batch.append((load_id, line_no, json.dumps(obj)))

            if len(batch) >= batch_size:
                cur.executemany(
                    "INSERT INTO raw.user_events(load_id, line_no, payload) VALUES (%s, %s, %s::jsonb) "
                    "ON CONFLICT DO NOTHING",
                    batch,
                )
                conn.commit()
                batch.clear()

    if batch:
        cur.executemany(
            "INSERT INTO raw.user_events(load_id, line_no, payload) VALUES (%s, %s, %s::jsonb) "
            "ON CONFLICT DO NOTHING",
            batch,
        )
        conn.commit()

    cur.close()
    conn.close()

with DAG(
    dag_id="dwh_raw_to_marts",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["dwh", "local", "postgres"],
) as dag:

    # (A) 로그 생성 (원하면 끄고 본인 파일 넣어도 됨)
    gen_logs = BashOperator(
        task_id="generate_logs",
        bash_command=(
            "python /opt/airflow/scripts/generate_user_behavior_logs.py "
            "--days 7 --users 20000 --out /opt/airflow/data/raw/events.jsonl"
        ),
    )

    # (B) 스키마/테이블 초기화
    init = PostgresOperator(
        task_id="init_ddl",
        postgres_conn_id="warehouse_pg",
        sql="/opt/airflow/sql/00_init.sql",
    )

    # (C) raw 적재
    load_raw = PythonOperator(
        task_id="load_raw_jsonl",
        python_callable=load_jsonl_to_raw,
    )

    # (D) staging
    staging = PostgresOperator(
        task_id="raw_to_staging",
        postgres_conn_id="warehouse_pg",
        sql="/opt/airflow/sql/10_staging.sql",
        params={"load_id": "{{ ti.xcom_pull(task_ids='load_raw_jsonl', key='load_id') }}"},
    )

    # (E) dims
    dim_user = PostgresOperator(
        task_id="upsert_dim_user_scd2",
        postgres_conn_id="warehouse_pg",
        sql="/opt/airflow/sql/20_dim_user_scd2.sql",
        params={"load_id": "{{ ti.xcom_pull(task_ids='load_raw_jsonl', key='load_id') }}"},
    )

    dim_product = PostgresOperator(
        task_id="upsert_dim_product",
        postgres_conn_id="warehouse_pg",
        sql="/opt/airflow/sql/21_dim_product.sql",
        params={"load_id": "{{ ti.xcom_pull(task_ids='load_raw_jsonl', key='load_id') }}"},
    )

    # (F) fact
    fact = PostgresOperator(
        task_id="load_fact",
        postgres_conn_id="warehouse_pg",
        sql="/opt/airflow/sql/30_fact.sql",
        params={"load_id": "{{ ti.xcom_pull(task_ids='load_raw_jsonl', key='load_id') }}"},
    )

    # (G) quality checks
    dq = PostgresOperator(
        task_id="data_quality_checks",
        postgres_conn_id="warehouse_pg",
        sql="/opt/airflow/sql/90_quality_checks.sql",
        params={"load_id": "{{ ti.xcom_pull(task_ids='load_raw_jsonl', key='load_id') }}"},
    )

    gen_logs >> init >> load_raw >> staging >> [dim_user, dim_product] >> fact >> dq
