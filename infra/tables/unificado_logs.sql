CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DATASET}.unificado_logs`
(
  fecha_proceso TIMESTAMP,
  ingestion_id STRING,
  filas_raw INT64,
  total_filas_tabla_unificado INT64,
  filas_duplicadas_removidas INT64,
  ruta_gcs STRING,
  archivo STRING,
  status STRING,
  error STRING
)
PARTITION BY DATE(fecha_proceso);