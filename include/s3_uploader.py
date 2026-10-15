import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import io
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET_NAME = 'nyc-taxi-data-richa'
AWS_REGION  = 'us-east-1'

# TLC publishes monthly yellow-taxi Parquet at predictable paths (no version in filename = "latest" for that month).
YELLOW_TRIPDATA_BASE = 'https://d37ci6vzurychx.cloudfront.net/trip-data'


def parquet_url_for_month(month: str) -> str:
    """Build public TLC URL for yellow_tripdata_{YYYY-MM}.parquet."""
    return f'{YELLOW_TRIPDATA_BASE}/yellow_tripdata_{month}.parquet'

def get_s3_client():
    """Create and return an S3 client."""
    return boto3.client('s3', region_name=AWS_REGION)


def download_parquet_as_dataframe(url: str) -> pd.DataFrame:
    """Download parquet file from URL into a DataFrame."""
    logger.info(f"Downloading: {url}")
    response = requests.get(url, timeout=120)
    response.raise_for_status()
    buffer = io.BytesIO(response.content)
    df = pd.read_parquet(buffer)
    logger.info(f"Downloaded {len(df):,} rows")
    return df


def add_metadata_columns(df: pd.DataFrame, month: str) -> pd.DataFrame:
    """Add pipeline metadata columns before loading."""
    df['ingestion_month']    = month
    df['ingested_at']        = datetime.utcnow().isoformat()
    df['source']             = 'nyc_tlc_yellow_taxi'
    return df


def upload_dataframe_to_s3(df: pd.DataFrame, s3_key: str, s3_client) -> str:
    """Convert DataFrame to parquet and upload directly to S3 (no local file)."""
    buffer = io.BytesIO()
    
    # Write parquet to in-memory buffer
    table = pa.Table.from_pandas(df)
    pq.write_table(table, buffer, compression='snappy')
    buffer.seek(0)
    
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=buffer.getvalue(),
        ContentType='application/octet-stream'
    )
    
    s3_uri = f's3://{BUCKET_NAME}/{s3_key}'
    logger.info(f"Uploaded → {s3_uri}")
    return s3_uri


def process_month(month: str, url: str, s3_client) -> dict:
    """Full pipeline for one month: download → enrich → upload."""
    try:
        # Download
        df = download_parquet_as_dataframe(url)
        
        # Add metadata
        df = add_metadata_columns(df, month)
        
        # Define S3 path using Hive-style partitioning
        year, mo   = month.split('-')
        s3_key     = f'raw/yellow_taxi/year={year}/month={mo}/data.parquet'
        
        # Upload
        s3_uri = upload_dataframe_to_s3(df, s3_key, s3_client)
        
        return {'month': month, 'status': 'success', 'rows': len(df), 's3_uri': s3_uri}

    except Exception as e:
        logger.error(f"Failed for {month}: {e}")
        return {'month': month, 'status': 'failed', 'error': str(e)}


def ingest_months(months: list[str]) -> list:
    """Download + upload each YYYY-MM in order using TLC canonical URLs."""
    s3_client = get_s3_client()
    results = []
    for month in months:
        url = parquet_url_for_month(month)
        results.append(process_month(month, url, s3_client))
    success = [r for r in results if r['status'] == 'success']
    failed = [r for r in results if r['status'] == 'failed']
    logger.info(f"\n✅ Success: {len(success)} | ❌ Failed: {len(failed)}")
    return results


def run_ingestion(months: list = None):
    """Back-compat: ingest explicit months, or all months in ``months`` if provided."""
    if not months:
        raise ValueError('run_ingestion(months=...) requires a list of YYYY-MM strings')
    return ingest_months(months)


if __name__ == '__main__':
    import sys
    months_cli = [a for a in sys.argv[1:] if a]
    if not months_cli:
        raise SystemExit('Usage: python s3_uploader.py YYYY-MM [YYYY-MM ...]')
    ingest_months(months_cli)


