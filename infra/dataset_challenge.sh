#!/bin/bash

source config.sh

echo "Creando dataset..."

bq --location=$REGION mk \
  --dataset \
  $PROJECT_ID:$DATASET

echo "Dataset creado: $DATASET"