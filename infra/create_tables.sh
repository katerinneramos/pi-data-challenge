#!/bin/bash

source config.sh

echo "Creando tablas en BigQuery..."

# Reemplazar variables en SQL y ejecutar
for file in tables/*.sql
do
  echo "Ejecutando $file..."

  sed "s/\${PROJECT_ID}/$PROJECT_ID/g; s/\${DATASET}/$DATASET/g" $file | \
  bq query --use_legacy_sql=false

done

echo "Tablas creadas correctamente"