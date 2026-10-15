import boto3
import redshift_connector
import logging
import os
from dotenv import load_dotenv

load_dotenv() 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# --- Config ---
REDSHIFT_HOST   = os.environ.get('REDSHIFT_HOST')
REDSHIFT_DB     = 'dev'
REDSHIFT_USER   = os.environ.get('REDSHIFT_USER')
REDSHIFT_PASS   = os.environ.get('REDSHIFT_PASSWORD')
REDSHIFT_PORT   = 5439
ssl = True
BUCKET_NAME     = 'nyc-taxi-data-richa'
IAM_ROLE_ARN    = os.environ.get('REDSHIFT_IAM_ROLE')  
# e.g. 'arn:aws:iam::123456789012:role/redshift-s3-access-role'


def get_redshift_connection():
    """Create and return a Redshift connection."""
    print(f"Connecting to -> Host: '{REDSHIFT_HOST}', Port: {REDSHIFT_PORT}")
    return redshift_connector.connect(
        host=REDSHIFT_HOST,
        database=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PASS,
        port=REDSHIFT_PORT
    )


def build_copy_command(year: str, month: str) -> str:
    """
    Build a COPY command targeting a specific Hive partition.
    Redshift will load ALL parquet files under that prefix.
    """
    s3_prefix = f's3://{BUCKET_NAME}/raw/yellow_taxi/year={year}/month={month}/'

    return f"""
        COPY taxi_staging.yellow_taxi_raw
        FROM '{s3_prefix}'
        IAM_ROLE '{IAM_ROLE_ARN}'
        FORMAT AS PARQUET
        SERIALIZETOJSON          
        ACCEPTINVCHARS           
        COMPUPDATE OFF           
        STATUPDATE OFF;          
    """
    # SERIALIZETOJSON  → handles nested/complex types gracefully
    # ACCEPTINVCHARS   → skips invalid UTF-8 chars instead of failing
    # COMPUPDATE OFF   → skip compression analysis, faster loads
    # STATUPDATE OFF   → skip stats update, run manually after bulk loads


def truncate_partition(cursor, year: str, month: str):
    """
    Delete existing rows for this partition before reloading.
    Makes the load idempotent — safe to re-run without duplicates.
    """
    logger.info(f"Truncating partition year={year}, month={month}")
    cursor.execute("""
        DELETE FROM taxi_staging.yellow_taxi_raw
        WHERE ingestion_month = %s
    """, (f'{year}-{month}',))


def load_partition(year: str, month: str) -> dict:
    """Full load for one year/month partition."""
    conn   = None
    cursor = None
    try:
        conn   = get_redshift_connection()
        cursor = conn.cursor()

        # Make load idempotent
        truncate_partition(cursor, year, month)

        # Build and run COPY
        copy_sql = build_copy_command(year, month)
        logger.info(f"Running COPY for year={year}, month={month}")
        logger.info(copy_sql)
        cursor.execute(copy_sql)

        # Verify row count
        cursor.execute("""
            SELECT COUNT(*) FROM taxi_staging.yellow_taxi_raw
            WHERE ingestion_month = %s
        """, (f'{year}-{month}',))
        row_count = cursor.fetchone()[0]
        logger.info(f"✅ Loaded {row_count:,} rows for {year}-{month}")

        conn.commit()
        return {'status': 'success', 'year': year, 'month': month, 'rows': row_count}

    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"❌ Load failed for {year}-{month}: {e}")
        return {'status': 'failed', 'year': year, 'month': month, 'error': str(e)}

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


def run_load(months: list = None):
    """
    Main entry point.
    months: list of 'YYYY-MM' strings, e.g. ['2024-01', '2024-02']
    """
    targets = months or ['2026-03', '2026-04', '2026-05']
    results = []

    for month_str in targets:
        year, month = month_str.split('-')
        result = load_partition(year, month)
        results.append(result)

    success = [r for r in results if r['status'] == 'success']
    failed  = [r for r in results if r['status'] == 'failed']
    logger.info(f"\nSummary → ✅ {len(success)} succeeded | ❌ {len(failed)} failed")
    return results


if __name__ == '__main__':
    run_load()
