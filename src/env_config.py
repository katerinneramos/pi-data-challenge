"""
Configuración de entorno: variables y constantes de infraestructura.
"""
import os

# Variables de entorno requeridas
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET = os.getenv("DATASET")
BUCKET_NAME = os.getenv("BUCKET_NAME")
SOURCE_CSV_URL = os.getenv("SOURCE_CSV_URL")

# Nombres de tablas medallion: raw >> int >> final
RAW_TABLE = f"{PROJECT_ID}.{DATASET}.unificado_raw"
INT_TABLE = f"{PROJECT_ID}.{DATASET}.unificado_int"
FINAL_TABLE = f"{PROJECT_ID}.{DATASET}.unificado"
LOGS_TABLE = f"{PROJECT_ID}.{DATASET}.unificado_logs"

# Esquema esperado del CSV
EXPECTED_COLUMNS = [
    'chrom', 'pos', 'id', 'ref', 'alt', 'qual', 'filter', 'info', 'format', 'muestra', 'valor', 'origen', 'resultado'
]
