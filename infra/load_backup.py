from google.cloud import bigquery
import os

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = os.getenv("DATASET")
BUCKET_NAME = os.getenv("BUCKET_NAME")

RAW_TABLE = f"{PROJECT_ID}.{DATASET}.unificado_raw"

bq_client = bigquery.Client()

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    autodetect=False,
)

uri = f"gs://{BUCKET_NAME}/raw/Unificado.csv"

load_job = bq_client.load_table_from_uri(uri, RAW_TABLE, job_config=job_config)
load_job.result()

print(f"Loaded {load_job.output_rows} rows into {RAW_TABLE}")