# PI Data Challenge - Presentación y Q&A

## RESUMEN EJECUTIVO

Solución de pipeline de datos en Google Cloud para cargar CSV periódicamente con registros duplicados y generar tabla final confiable.

**Arquitectura:** Medallion (RAW → INT → FINAL) + BigQuery + Cloud Functions + GCS + Cloud Scheduler

**Tiempo:** 10-12 horas (incluye refactorización modular + documentación)

---

## ANÁLISIS DEL PROBLEMA

### Requerimientos identificados

1. **Carga periódica** → automatización con Cloud Scheduler
2. **Datos duplicados** → deduplicación idempotente
3. **Reproceso seguro** → no romper tabla final
4. **Auditoría** → logs de cada ejecución
5. **Escalabilidad** → listo para crecer

### ¿Por qué Google Cloud?

| Opción | Razón de descarte |
|--------|------------------|
| Data Factory | Overkill para este volumen |
| Dataflow | Más caro y complejo de lo necesario |
| Dataproc | Excesivo, requiere cluster permanente |
| **BigQuery + Cloud Functions** | ✅ Simple, escalable, serverless, económico |

---

## DECISIONES ARQUITECTÓNICAS

### 1. Patrón Medallion: RAW → INT → FINAL

**RAW (Capa bronce):**
- Histórico técnico exacto de lo que llegó del CSV
- Permite auditar, revisar, reprocessar
- Si hay errores, tengo evidencia

**INT (Capa plata):**
- Transformaciones: tipos (pos STRING→INT64, qual STRING→FLOAT64)
- Metadatos: ingestion_id, source_file para trazabilidad
- Con MERGE para permitir reproceso idempotente

**FINAL (Capa oro):**
- Tabla de consumo, un registro por (id, muestra, resultado)
- Mantiene el más reciente
- Con CREATE OR REPLACE para reconstrucción completa

**Ventaja:** Si descubren un bug, reprocesso desde RAW sin perder nada.

### 2. MERGE vs CREATE OR REPLACE

- **MERGE en INT:** Idempotente. Si corro dos veces el mismo ingestion_id, hace UPDATE, no INSERT.
- **CREATE OR REPLACE en FINAL:** Reconstrucción completa garantiza consistencia. Estado consolidado.

### 3. ROW_NUMBER() para Deduplicación

```sql
ROW_NUMBER() OVER (PARTITION BY id, muestra, resultado ORDER BY fecha_copia DESC)
WHERE rownumber = 1
```

**¿Por qué no GROUP BY + MAX?**
- ROW_NUMBER es más eficiente en BigQuery
- El intent es más claro: "dame una fila por grupo"
- Más fácil de extender si necesito más lógica

---

## IMPLEMENTACIÓN: CÓDIGO MODULAR

### Estructura de módulos

```
main.py (Orquestador)
    │
    ├─→ env_config.py (Config)
    ├─→ data_processing.py (Lógica)
    │   └─→ queries.py (SQL)
    └─→ Funciones de soporte (descarga, upload, logs)
```

### Módulo por módulo

**env_config.py:** Single source of truth
- PROJECT_ID, DATASET, BUCKET_NAME, SOURCE_CSV_URL
- EXPECTED_COLUMNS para validación

**queries.py:** SQL separado de Python
- `create_int_table_query(int_table)`
- `merge_int_table_query(int_table, raw_table, ingestion_id, source_file)`
- `create_final_table_query(final_table, int_table)`

**data_processing.py:** Medallion logic
- `load_to_raw(file_path, ingestion_id)`
- `build_intermediate(ingestion_id, source_file)`
- `build_final()`

**main.py:** Orquest y coordinación
- `validate_dataset()`, `validate_runtime_config()`
- `download_csv()`, `upload_to_gcs()`, `insert_log()`
- `main()` entry point

**Beneficios:**
- ✅ Bajo acoplamiento, alta cohesión
- ✅ Fácil de testear y mantener
- ✅ Reutilizable en contextos diferentes

---

## DIFICULTADES ENCONTRADAS

### 1. MERGE con múltiples rows del mismo source

**Problema:** BigQuery error si varias filas de source matchean la misma fila target

**Solución:** ROW_NUMBER() ANTES del MERGE
```sql
SELECT ... FROM (
    SELECT *,
    ROW_NUMBER() OVER (PARTITION BY id, muestra, resultado ORDER BY fecha_copia DESC) AS rn
    FROM raw_table
) WHERE rn = 1
```

### 2. Tipos de datos: Pandas → BigQuery

**Problema:** Pandas infiere tipos, BigQuery tiene reglas distintas, PyArrow en el medio

**Solución:** Leer TODO como STRING, luego CAST explícito en BigQuery
```python
df = pd.read_csv(file_path, dtype=str)  # TODO string
# En SQL: CAST(pos AS INT64), CAST(qual AS FLOAT64)
```

### 3. Validación de obligatoriedad

**Problema:** Si falta id, muestra, resultado no puedo desduplicar

**Solución:** Validar ANTES de cargar, fallar temprano con mensaje claro
```python
required_fields = ['id', 'muestra', 'resultado']
for field in required_fields:
    if df[field].isna().any() or (df[field] == '').any():
        raise Exception(f"Campo '{field}' contiene NULL")
```

---

## RESPUESTAS A PREGUNTAS FRECUENTES

### P: ¿Qué harías diferente?

**Respuesta:**

1. **Parámetrización total:** Nombres de tabla, esquema, lógica de deduplicación como variables
2. **Error handling granular:** Hoy un error detiene todo. Podría logging parcial y continuar
3. **Compresión en GCS:** Guardar CSV comprimido (gzip) para reducir storage
4. **Versionado de esquema:** Si el CSV cambia de estructura, detectar y alertar
5. **Testing automatizado:** Unitarios + integración con BigQuery emulator

Local: Habría agregado tests desde el inicio, no al final.

---

### P: ¿Cómo testearías esto?

**Respuesta:**

**1. Unitarios (sin Cloud):**
```python
# tests/test_queries.py
def test_create_int_table_query():
    from src.queries import create_int_table_query
    query = create_int_table_query("test_table")
    assert "CREATE TABLE IF NOT EXISTS" in query
    assert "pos INT64" in query
    assert "PARTITION BY DATE(fecha_copia)" in query
```

**2. Integración local (con mock de BigQuery):**
```python
from unittest.mock import patch

@patch('google.cloud.bigquery.Client')
def test_load_to_raw(mock_bq):
    mock_bq.load_table_from_dataframe = MagicMock()
    result = load_to_raw("test.csv", "test-id")
    assert result == expected_row_count
```

**3. Integración real (contra dev dataset):**
```bash
# Crear dataset temporal, correr pipeline completo, verificar
pytest tests/test_e2e.py --gcp-project=dev
```

**4. Data validation:**
```sql
-- Verificar deduplicación
SELECT id, muestra, resultado, COUNT(*) 
FROM final_table 
GROUP BY 1,2,3 
HAVING COUNT(*) > 1;  -- Debería retornar 0 filas

-- Verificar no hay NULL en keys
SELECT COUNT(*) FROM final_table 
WHERE id IS NULL OR muestra IS NULL OR resultado IS NULL;
```

---

### P: ¿Qué pasa si el CSV es 1GB?

**Respuesta:**

**Hoy (512MB, Python):**
- Cloud Function falla por memoria insuficiente
- Timeout posible si descarga toma > 540 segundos

**Soluciones:**

1. **Aumentar memoria CF:** Hasta 8GB
2. **Procesamiento streaming:** En lugar de cargar TODO en memoria
   ```python
   for chunk in pd.read_csv(file_path, chunksize=10000, dtype=str):
       bq_client.load_table_from_dataframe(chunk, RAW_TABLE, job_config=...)
   ```

3. **BigQuery Data Transfer Service:** Para cargas periódicas masivas
   - Maneja directamente desde GCS → BigQuery
   - No pasa por Python

4. **Compresión y particionado:**
   - Descargar en partes
   - Procesar en paralelo con múltiples CF (Pub/Sub)

5. **Firestore/Cache:**
   - Si el CSV es muy grande pero cambian pocos registros
   - Descargar delta en lugar de completo

**Recomendación senior:** Pasar a BigQuery Data Transfer Service, no CF.

---

### P: ¿Y si la deduplicación falla?

**Respuesta:**

**Escenarios de falla:**

1. **Hay duplicados en (id, muestra, resultado)** → ROW_NUMBER toma el más reciente, conforme a spec
2. **Hay NULL en keys** → Validación en load_to_raw() lanza excepción, log error, reintento
3. **MERGE falla porque están bloqueadas las tablas** → Retry con backoff exponencial
4. **Lógica de fecha_copia es inconsistente** → Verificar SQL, re-ejecutar desde RAW

**Prevención:**

```python
# Post-proceso: verificar deduplicación
final_count = bq_client.query(f"SELECT COUNT(*) FROM {FINAL_TABLE}").result()
unique_keys = bq_client.query(
    f"SELECT COUNT(DISTINCT CONCAT(id, muestra, resultado)) FROM {FINAL_TABLE}"
).result()

# Si final_count == unique_keys, OK
# Si no, hay un problema lógico
assert final_count == unique_keys, "Deduplicación falló"
```

**Alertas automáticas:**
- Si fila duplicada en FINAL → Slack alert
- Si tiempo de proceso > 2 std_dev → Investigar
- Si error_log.count > 0 → Email al equipo

---

### P: ¿Puede correr sola la Cloud Function?

**Respuesta:**

**Sí, totalmente. Tienes 3 formas:**

#### Opción 1: Cloud Scheduler (Hoy)
```bash
gcloud scheduler jobs create pubsub pi-data-challenge-weekly \
  --location=us-central1 \
  --schedule="0 5 * * 1"  # Lunes 5 AM UTC \
  --topic=pi-data-trigger
  
# Cloud Function escucha el Pub/Sub topic y se ejecuta
```

#### Opción 2: Disparada por eventos en GCS
```hcl
# Terraform: Cloud Function dispara cuando archivo llega a bucket
resource "google_cloudfunctions_function" "pipeline" {
  event_trigger {
    event_type = "google.storage.object.finalize"
    resource   = google_storage_bucket.raw.name
  }
  # Cuando alguien sube archivo a gs://bucket/nuevas_filas.csv
  # → CF se ejecuta automáticamente
}
```

#### Opción 3: HTTP trigger manual
```bash
curl -X POST "https://us-central1-PROJECT.cloudfunctions.net/pi-data-challenge-pipeline" \
  -H "Authorization: bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d '{}'
```

**Recomendación:**
- Desarrollo: triggers manuales (HTTP)
- Producción: Scheduler (predecible) + alertas en error
- Futuro: Event-driven si hay múltiples fuentes

---

## MÁS PREGUNTAS POSIBLES

### Gestión de errores
- **P: ¿Qué pasa si la URL del CSV no responde?**
  - R: `requests.get(..., timeout=30)` lanza excepción, capturada, log error, alert

- **P: ¿Qué pasa si BigQuery está en mantenimiento?**
  - R: Retry con backoff exponencial. Si persiste > 3 intentos, fallar y loguear

- **P: ¿Cómo recuperarse de un error mid-pipeline?**
  - R: Log ingestion_id. Corregir, reprocessar desde RAW sin tocar tabla FINAL

### Performance
- **P: ¿Cuánto tarda en procesar 100K filas?**
  - R: Típicamente 30-60 segundos (descarga + validación + carga + MERGE + FINAL)
  - Si toma > 2 min, optimizar: índices, particionado, CLUSTER BY

- **P: ¿Cuál es el costo aproximado?**
  - R: BigQuery ~$0.03 per GB scanned. Para 100MB ≈ $0.003. Cloud Functions: ≈ $0.40/millón calls

### Escalabilidad
- **P: ¿Y si necesito procesar 5 fuentes distintas?**
  - R: Parámetrizar: `main(source_config)` que recibe nombre tabla, CSV URL, lógica dedup

- **P: ¿Y si el CSV cambia de estructura?**
  - R: Versionado de esquema. En load_to_raw(), verificar EXPECTED_COLUMNS contra esquema real

### Seguridad
- **P: ¿Cómo protejo credenciales?**
  - R: Usar Secret Manager (no env vars en producción)
  - IA Identity-Aware Proxy en CF si es cliente frecuente

- **P: ¿Cómo audito quiénes ejecutaron la CF?**
  - R: Cloud Audit Logs registra cada invocación con identity, timestamp, resultado

### Operacional
- **P: ¿Cómo monitoreó el pipeline?**
  - R: Cloud Monitoring dashboards + alertas. Datadog para histórico largo

- **P: ¿Cómo hago rollback si algo salió mal?**
  - R: DELETE de FINAL y INT, reprocesso desde RAW. La tabla RAW es el "source of truth"

---

## INFRAESTRUCTURA COMO CÓDIGO: Terraform

**Hoy:** Scripts bash (no reproducible)
**Propuesta:** Terraform (reproducible, versionado, multi-ambiente)

```hcl
# Terraform: Toda la infra en código
resource "google_bigquery_dataset" "challenge" {
  dataset_id = var.dataset_id
  location   = var.region
}

resource "google_storage_bucket" "raw" {
  name     = var.bucket_name
  location = var.region
}

resource "google_cloudfunctions_function" "pipeline" {
  name        = "pi-data-challenge-pipeline"
  runtime     = "python311"
  ...
}

resource "google_cloud_scheduler_job" "trigger" {
  schedule = "0 5 * * 1"  # Lunes 5 AM
  ...
}
```

**Beneficios:**
- ✅ Reproducible: destroyo/recreo en 5 min
- ✅ Versionado: Git history de cambios infra
- ✅ Multi-ambiente: dev.tfvars, prod.tfvars
- ✅ CI/CD: `terraform apply` en pipeline

---

## NIVEL SENIOR: REFLEXIÓN

¿Qué hace esto "nivel senior"?

1. **Simplicidad con profundidad** → No sobre-engineericé. Elegí exactamente lo que necesitaba.

2. **Pensamiento operacional** → Pensé en reproceso, auditoría, escalabilidad, recuperación.

3. **Código limpio** → Modularizado, documentado, mantenible por otros.

4. **Decisiones justificadas** → Cada tecnología tiene un "por qué".

5. **Visión futura** → Identifiqué puntos de escalabilidad sin construirlos hoy *(YAGNI)*

6. **Mentalidad de product** → No solo "que funcione", sino "que sea mantenible y escalable"

---

## TIPS PARA LA PRESENTACIÓN EN VIVO

1. **Habla lento:** Es fácil irse rápido. Pausa entre ideas.
2. **Mira al entrevistador:** No solo la pantalla.
3. **Sé honesto:** "Me tomó X porque..." // "Consideré Y pero elegí Z porque..."
4. **Demo viva:**
   - Muestra estructura de archivos
   - Ejecuta query desde Python
   - Muestra README en GitHub
5. **Si algo falla:** Ctrl+Z, respira, sigue. Es normal.
6. **Cierra fuerte:** Resume en 2 frases qué lo hace profesional.

---

## CONCLUSIÓN

✅ Solución simple y robusta
✅ Código profesional y mantenible
✅ Preparado para escala
✅ Infraestructura reproducible
✅ Documentación completa

**Pregunta final:** ¿Preguntas?
