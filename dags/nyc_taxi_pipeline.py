import pendulum
from airflow import DAG
from airflow.exceptions import AirflowFailException
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta

from include.s3_uploader import ingest_months
from include.redshift_loader import run_load

default_args = {
    'owner': 'richa',
    'retries': 1,
    'retry_delay': timedelta(minutes=4),
}

def extract_taxi_data(**context):
    """Ingest prior calendar month (TLC publishes with lag). Override via DagRun conf."""
    print("Extracting NYC Taxi data from S3...")
    conf = (context.get('dag_run') and context['dag_run'].conf) or {}
    if conf.get('ingest_month'):
        execution_month = conf['ingest_month']
    else:
        run_day = pendulum.parse(context['ds'])
        execution_month = run_day.subtract(months=7).format('YYYY-MM')

    results = ingest_months([execution_month])
    if not results:
        raise AirflowFailException(
            f"run_ingestion returned no work for {execution_month!r}; nothing was uploaded to S3."
        )
    failures = [r for r in results if r.get("status") != "success"]
    if failures:
        raise AirflowFailException(
            f"Ingestion/upload did not succeed for {execution_month!r}: {failures}"
        )
    return results

def load_to_redshift(**context):
    print("Loading data into Redshift staging...")
    conf = (context.get('dag_run') and context['dag_run'].conf) or {}
    if conf.get('ingest_month'):
        execution_month = conf['ingest_month']
    else:
        run_day = pendulum.parse(context['ds'])
        execution_month = run_day.subtract(months=7).format('YYYY-MM')

    results = run_load([execution_month])
    if not results:
        raise AirflowFailException(
            f"run_load returned no work for {execution_month!r}; nothing was uploaded to redshift."
        )
    failures = [r for r in results if r.get("status") != "success"]
    if failures:
        raise AirflowFailException(
            f"Upload did not succeed for {execution_month!r}: {failures}"
        )
    return results

with DAG(
    dag_id='nyc_taxi_pipeline',
    default_args=default_args,
    description='NYC Taxi: monthly ingest of prior month TLC parquet → S3 (tune day-of-month for TLC release).',
    # 08:00 UTC on the 8th — after TLC usually drops prior-month files; adjust if you see 403/404.
    schedule='0 8 8 * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['nyc-taxi', 'portfolio'],
) as dag:

    extract = PythonOperator(
        task_id='extract_taxi_data',
        python_callable=extract_taxi_data,
    )

    load = PythonOperator(
        task_id='load_to_redshift',
        python_callable=load_to_redshift,
    )

    extract >> load  # extract runs first, then load
