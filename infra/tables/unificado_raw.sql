CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DATASET}.unificado_raw`
(
  chrom STRING,
  pos STRING,
  id STRING,
  ref STRING,
  alt STRING,
  qual STRING,
  filter STRING,
  info STRING,
  format STRING,
  muestra STRING,
  valor STRING,
  origen STRING,
  fecha_copia TIMESTAMP,
  resultado STRING
)
PARTITION BY DATE(fecha_copia);