#!/bin/bash
# Script para crear Cloud Scheduler que ejecute la función cada lunes a las 5:00 AM

SCHEDULER_NAME="pi-data-challenge-weekly"
FUNCTION_NAME="pi-data-challenge-pipeline"
PROJECT_ID="pi-data-engineer-challenge"
REGION="us-central1"
SCHEDULE="0 5 * * 1"  # 5:00 AM todos los lunes (formato cron UTC)
TIMEZONE="Etc/UTC"

echo "Creando Cloud Scheduler..."

gcloud scheduler jobs create http $SCHEDULER_NAME \
  --location=$REGION \
  --schedule="$SCHEDULE" \
  --time-zone="$TIMEZONE" \
  --uri="https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME" \
  --http-method=POST \
  --project=$PROJECT_ID \
  --oidc-service-account-email="$PROJECT_ID@appspot.gserviceaccount.com" \
  --oidc-token-audience="https://$REGION-$PROJECT_ID.cloudfunctions.net/$FUNCTION_NAME"

echo "Cloud Scheduler '$SCHEDULER_NAME' creado exitosamente"
echo "Ejecutará: lunes a las 5:00 AM (UTC)"
