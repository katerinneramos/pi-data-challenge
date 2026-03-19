"""
Data Processing: lógica medallion architecture.
Transformación de datos: raw >> int >> final
"""

import logging
import pandas as pd
from datetime import datetime
from google.cloud import bigquery

from env_config import RAW_TABLE, INT_TABLE, FINAL_TABLE, EXPECTED_COLUMNS
from queries import (
    get_create_int_table_query,
    get_merge_int_table_query,
    get_create_final_table_query
)

logger = logging.getLogger(__name__)
bq_client = bigquery.Client()


def load_to_raw(file_path, ingestion_id):
    """
    Cargamos el CSV a la tabla RAW.
    - Lectura desde CSV
    - Validación de columnas y campos obligatorios
    - Carga a BigQuery en tabla RAW
    """
    logger.info("Cargando CSV a tabla unificado_raw")

    df = pd.read_csv(file_path, dtype=str)
    df.columns = [col.lower() for col in df.columns]

    df["fecha_copia"] = datetime.utcnow()

    # Validaciones de estructura
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
    Crear tabla INT en base a RAW.
    - Agregamos: ingestion_id, source_file
    - Cambio en data_type: pos INT64, qual FLOAT64
    - Deduplicación: mantiene un registro por (id, muestra, resultado)
    """
    logger.info("Create tabla unificado_int desde unificado_raw.")

    # Crear tabla INT si no existe
    create_query = get_create_int_table_query(INT_TABLE)
    bq_client.query(create_query).result()

    # Merge con deduplicación
    merge_query = get_merge_int_table_query(INT_TABLE, RAW_TABLE, ingestion_id, source_file)
    bq_client.query(merge_query).result()
    
    logger.info(f"Tabla unificado_int actualizada con ingestion_id: {ingestion_id}")


def build_final():
    """
    CreaMOS tabla final en base a unificado_int.
    - Solo casos únicos: id + muestra + resultado
    - Mantenemos último registro (fecha_copia DESC)
    """
    logger.info("Create tabla unificado desde unificado_int, removiendo duplicados.")

    raw_total_count = list(bq_client.query(
        f"SELECT COUNT(*) c FROM `{RAW_TABLE}`"
    ).result())[0].c

    # Generar query para tabla FINAL
    final_query = get_create_final_table_query(FINAL_TABLE, INT_TABLE)
    bq_client.query(final_query).result()

    final_count = list(bq_client.query(
        f"SELECT COUNT(*) c FROM `{FINAL_TABLE}`"
    ).result())[0].c

    # Filas del histórico en RAW que quedan removidas por ser duplicados
    row_removed = raw_total_count - final_count

    logger.info(
        f"Tabla unificado creada: {final_count} registros, "
        f"{row_removed} duplicados removidos"
    )
    return final_count, row_removed
