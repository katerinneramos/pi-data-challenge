import os
import uuid
import logging
import pandas as pd
from datetime import datetime
from google.cloud import storage, bigquery
from google.api_core.exceptions import NotFound
import requests
import time

PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = os.getenv("DATASET")
BUCKET_NAME = os.getenv("BUCKET_NAME")

URL = "https://storagechallengede.blob.core.windows.net/challenge/nuevas_filas%201.csv?sp=r&st=2026-02-04T14:26:43Z&se=2026-12-31T22:41:43Z&sv=2024-11-04&sr=b&sig=9%2Fnsh4qSw6E6fDkdEx8QLBH7kKlC4szAv2Z%2F%2BmLLF4A%3D"

# Tablas medallion: raw >> int >> final
RAW_TABLE = f"{PROJECT_ID}.{DATASET}.unificado_raw"
INT_TABLE = f"{PROJECT_ID}.{DATASET}.unificado_int" 
FINAL_TABLE = f"{PROJECT_ID}.{DATASET}.unificado"
LOGS_TABLE = f"{PROJECT_ID}.{DATASET}.unificado_logs"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bq_client = bigquery.Client()
storage_client = storage.Client()

EXPECTED_COLUMNS = ['chrom', 'pos', 'id', 'ref', 'alt', 'qual', 'filter', 'info', 'format', 'muestra', 'valor', 'origen', 'resultado']

def validate_dataset():
    try:
        bq_client.get_dataset(DATASET)
        logger.info(f"Dataset {DATASET}: existe.")
    except NotFound:
        raise Exception("El Dataset no existe.")

def download_csv():
    logger.info("Descargando CSV.")
    response = requests.get(URL, timeout=30)
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
    Cargamos el CSV a la tabla RAW
    """
    logger.info("Cargando CSV a tabla unificado_raw")

    df = pd.read_csv(file_path, dtype=str)
    df.columns = [col.lower() for col in df.columns]

    df["fecha_copia"] = datetime.utcnow()

    # Validaciones
    missing_cols = set(EXPECTED_COLUMNS) - set(df.columns)
    if missing_cols:
        raise Exception(f"Columnas faltantes: {missing_cols}")
    if len(df) == 0:
        raise Exception("CSV sin filas")
    
    # Validar que campos de deduplicación no sean vacíos
    required_fields = ['id', 'muestra', 'resultado']
    for field in required_fields:
        if df[field].isna().any() or (df[field] == '').any():
            raise Exception(f"Campo '{field}' contiene valores vacíos o NULL, es requerido para deduplicación")

    # Cargar csv a RAW
    job = bq_client.load_table_from_dataframe(df, RAW_TABLE)
    job.result()

    logger.info(f"RAW cargada: {len(df)} filas con ingestion_id: {ingestion_id}")
    return len(df)

def build_intermediate(ingestion_id, source_file):
    """
    Crear tabla INT en base a RAW
    - Agregamos: ingestion_id, source_file
    - Cambio en data_type: pos INT64, qual FLOAT64
    """
    logger.info("Create tabla unificado_int desde unificado_raw.")

    # Crear tabla INT si no existe
    create_if_not_exists = f"""
    CREATE TABLE IF NOT EXISTS `{INT_TABLE}` (
        chrom STRING,
        pos INT64,
        id STRING,
        ref STRING,
        alt STRING,
        qual FLOAT64,
        filter STRING,
        info STRING,
        format STRING,
        muestra STRING,
        valor STRING,
        origen STRING,
        fecha_copia TIMESTAMP,
        resultado STRING,
        ingestion_id STRING,
        source_file STRING
    )
    PARTITION BY DATE(fecha_copia)
    """
    bq_client.query(create_if_not_exists).result()

    merge_query = f"""
    MERGE `{INT_TABLE}` AS int
    USING (
        SELECT
            chrom,
            pos,
            id,
            ref,
            alt,
            qual,
            filter,
            info,
            format,
            muestra,
            valor,
            origen,
            fecha_copia,
            resultado,
            ingestion_id,
            source_file
        FROM (
            SELECT 
                chrom,
                CAST(pos AS INT64) AS pos,
                id,
                ref,
                alt,
                CAST(qual AS FLOAT64) AS qual,
                filter,
                info,
                format,
                muestra,
                valor,
                origen,
                fecha_copia,
                resultado,
                '{ingestion_id}' AS ingestion_id,
                '{source_file}' AS source_file,
                ROW_NUMBER() OVER (PARTITION BY id, muestra, resultado ORDER BY fecha_copia DESC) AS rownumber
            FROM `{RAW_TABLE}`
        )
        WHERE rownumber = 1
    ) AS raw
    ON int.id = raw.id
       AND int.muestra = raw.muestra
       AND int.resultado = raw.resultado
    
    WHEN MATCHED THEN 
    UPDATE SET 
        chrom = raw.chrom,
        pos = raw.pos,
        ref = raw.ref,
        alt = raw.alt,
        qual = raw.qual,
        filter = raw.filter,
        info = raw.info,
        format = raw.format,
        valor = raw.valor,
        origen = raw.origen,
        fecha_copia = raw.fecha_copia,
        ingestion_id = raw.ingestion_id,
        source_file = raw.source_file
    
    WHEN NOT MATCHED THEN 
    INSERT (
        chrom, pos, id, ref, alt, qual, filter, info, format,
        muestra, valor, origen, fecha_copia, resultado,
        ingestion_id, source_file
    )
    VALUES (
        raw.chrom, raw.pos, raw.id, raw.ref, raw.alt, raw.qual, raw.filter, raw.info, raw.format,
        raw.muestra, raw.valor, raw.origen, raw.fecha_copia, raw.resultado,
        raw.ingestion_id, raw.source_file
    );
    """
    bq_client.query(merge_query).result()
    logger.info(f"Tabla unificado_int actualizada con ingestion_id: {ingestion_id}")

def build_final():
    """
    CreaMOS tabla final en base a unificado_int
    - Solo casos únicos: id + muestra + resultado
    - Mantenemos último registro (fecha_copia DESC)
    """
    logger.info("Create tabla unificado desde unificado_int, removiendo duplicados.")

    count_before = list(bq_client.query(
        f"SELECT COUNT(*) c FROM `{INT_TABLE}`"
    ).result())[0].c

    query = f"""
    CREATE OR REPLACE TABLE `{FINAL_TABLE}`
    PARTITION BY DATE(fecha_copia) AS
    SELECT
        chrom,
        pos,
        id,
        ref,
        alt,
        qual,
        filter,
        info,
        format,
        muestra,
        valor,
        origen,
        fecha_copia,
        resultado,
        ingestion_id,
        source_file
    FROM (
        SELECT *,
        ROW_NUMBER() OVER(PARTITION BY id, muestra, resultado ORDER BY fecha_copia DESC) rownumber
        FROM `{INT_TABLE}`
    )
    WHERE rownumber = 1
    """

    bq_client.query(query).result()

    count_after = list(bq_client.query(
        f"SELECT COUNT(*) c FROM `{FINAL_TABLE}`"
    ).result())[0].c
    # Vemos los duplicados removidos
    row_removed = count_before - count_after

    logger.info(
        f"Tabla unificado creada: {count_after} registros, "
        f"{row_removed} duplicados removidos"
    )
    return count_after, row_removed

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