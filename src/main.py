import os
import uuid
import logging
import time
from datetime import datetime
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound
import requests

# Importar módulos de la arquitectura modular
from env_config import (
    PROJECT_ID, DATASET, BUCKET_NAME, SOURCE_CSV_URL,
    RAW_TABLE, INT_TABLE, FINAL_TABLE, LOGS_TABLE,
    EXPECTED_COLUMNS
)
from data_processing import (
    load_to_raw,
    build_intermediate,
    build_final
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bq_client = bigquery.Client()
storage_client = storage.Client()

def validate_dataset():
    try:
        bq_client.get_dataset(DATASET)
        logger.info(f"Dataset {DATASET}: existe.")
    except NotFound:
        raise Exception("El Dataset no existe.")

def validate_runtime_config():
    missing_vars = []

    if not PROJECT_ID:
        missing_vars.append("PROJECT_ID")
    if not DATASET:
        missing_vars.append("DATASET")
    if not BUCKET_NAME:
        missing_vars.append("BUCKET_NAME")
    if not SOURCE_CSV_URL:
        missing_vars.append("SOURCE_CSV_URL")

    if missing_vars:
        raise Exception(f"Faltan variables de entorno requeridas: {', '.join(missing_vars)}")

def download_csv():
    logger.info("Descargando CSV.")
    response = requests.get(SOURCE_CSV_URL, timeout=30)
    response.raise_for_status()

    if not response.content:
        raise Exception("El CSV está vacío")

    file_path = f"/tmp/nuevas_filas_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(file_path, "wb") as f:
        f.write(response.content)

    logger.info(f"CSV descargado: {len(response.content)} bytes")
    return file_path

def upload_to_gcs(file_path, ingestion_id):
    today = datetime.utcnow().strftime("%Y/%m/%d")
    blob_name = f"raw/{today}/{ingestion_id}.csv"

    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(file_path)

    logger.info(f"Archivo subido: gs://{BUCKET_NAME}/{blob_name}")
    return blob_name


def load_to_raw(file_path, ingestion_id):
    """
    Procesamiento en tabla raw ver en: data_processing.py >> load_to_raw
    """
    from data_processing import load_to_raw as dp_load_to_raw
    return dp_load_to_raw(file_path, ingestion_id)


def build_intermediate(ingestion_id, source_file):
    """
    Procesamiento en tabla intermedia ver en: data_processing.py >> build_intermediate
    """
    from data_processing import build_intermediate as dp_build_intermediate
    return dp_build_intermediate(ingestion_id, source_file)

def build_final():
    """
    Procesamiento en tabla final ver en: data_processing.py >> build_final
    """
    from data_processing import build_final as dp_build_final
    return dp_build_final()

def insert_log(ingestion_id, raw_count, final_count, row_removed, file_name, gcs_path, status, error=None):
    try:
        now = datetime.utcnow()
        row = [{
            "fecha_proceso": now.strftime("%Y-%m-%d %H:%M:%S"),
            "ingestion_id": ingestion_id,
            "filas_raw": raw_count,
            "total_filas_tabla_unificado": final_count,
            "filas_duplicadas_removidas": row_removed,
            "archivo": file_name,
            "ruta_gcs": f"gs://{BUCKET_NAME}/{gcs_path}",
            "status": status,
            "error": error
        }]

        errors = bq_client.insert_rows_json(LOGS_TABLE, row)
        if errors:
            logger.error(f"Errores al insertar log: {errors}")
        else:
            logger.info("Log insertado exitosamente")
    except Exception as e:
        logger.error(f"Error insertando log: {str(e)}", exc_info=True)

def main(request=None):
    start = time.time()
    ingestion_id = str(uuid.uuid4())
    raw_count = 0
    final_count = 0
    row_removed = 0
    file_name = ""

    try:
        logger.info(f"Proyecto: {PROJECT_ID}, Dataset: {DATASET}")

        validate_runtime_config()

        validate_dataset()

        # Descargar CSV
        file_path = download_csv()
        file_name = file_path

        # Subir csv a GCS
        blob_name = upload_to_gcs(file_path, ingestion_id)

        # Cargar a RAW
        raw_count = load_to_raw(file_path, ingestion_id)

        # Crear INT con metadatos
        build_intermediate(ingestion_id, file_name)

        # Crea tabla FINAL deduplicada
        final_count, row_removed = build_final()

        # Insertar logs
        insert_log(
            ingestion_id,
            raw_count,
            final_count,
            row_removed,
            file_name,
            blob_name,
            "SUCCESS"
        )
        duration = time.time() - start
        logger.info(f"Proceso completado exitosamente en {duration:.2f} segundos")
        return {
            "status": "OK",
            "ingestion_id": ingestion_id,
            "raw_rows": raw_count,
            "final_rows": final_count,
            "row_removed": row_removed,
            "duration_seconds": round(duration, 2)
        }, 200

    except Exception as e:
        logger.error(f"ERROR: {str(e)}", exc_info=True)

        insert_log(
            ingestion_id,
            raw_count,
            final_count,
            row_removed,
            file_name,
            blob_name if 'blob_name' in locals() else "N/A",
            "ERROR",
            str(e)
        )
        duration = time.time() - start

        return {
            "status": "ERROR",
            "error": str(e),
            "ingestion_id": ingestion_id,
            "duration_seconds": round(duration, 2)
        }, 500