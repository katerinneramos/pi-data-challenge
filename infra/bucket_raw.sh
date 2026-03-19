#!/bin/bash

source config.sh

echo "Creando bucket..."

gsutil mb -p $PROJECT_ID -l $REGION gs://$BUCKET_NAME

echo "Bucket creado: gs://$BUCKET_NAME"