-- SILVER LAYER: Datos transformados con metadatos
-- - Tipos convertidos: pos INT64, qual FLOAT64
-- - Metadatos agregados: ingestion_id, source_file
-- - Listo para deduplicación y downstream processing
-- - Particionado por date(fecha_copia)

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DATASET}.unificado_int`
(
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
PARTITION BY DATE(fecha_copia);
