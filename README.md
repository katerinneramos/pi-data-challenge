# PI Data Engineer Challenge - Solución en Google Cloud

## Resumen del proyecto

Esta solución implementa un pipeline de datos en **Google Cloud Platform** para procesar archivos CSV de datos y generar tablas finales deduplicadas en **BigQuery**.  
El flujo sigue un patrón tipo **Medallion Architecture**: RAW >> INTERMEDIATE >> FINAL, con logs de procesamiento para trazabilidad.

---

## Arquitectura de la solución

![Arquitectura GCP](img/arch-pipeline-pi-data.png)

1. **Cloud Storage (GCS)**  
   - Contiene la archivo CSV en crudo (`raw/`) y registros de ingestión.  
   - Para respaldo del archivo fuente y trazabilidad.

2. **Cloud Functions**  
   - Función principal: `pi-data-challenge-pipeline`
   - Desencadenada por HTTP o Scheduler para ejecuciones automáticas.  
   - Realiza todo el pipeline: descarga CSV >> carga a RAW >> transformación >> tabla FINAL >> tabla LOGS.

3. **BigQuery**  
   - **RAW Table** (`unificado_raw`): carga inicial del CSV, sin transformaciones.  
   - **INTERMEDIATE Table** (`unificado_int`): tipado de columnas, agregación de metadatos (`ingestion_id`, `source_file`) y deduplicación parcial.  
   - **FINAL Table** (`unificado`): tabla deduplicada final, lista para análisis y reporting.  
   - **LOGS Table** (`unificado_logs`): historial de ejecuciones con métricas y errores.

4. **Cloud Scheduler**
   - Job `pi-data-challenge-weekly` que ejecuta la función cada lunes a las 5:00 AM (UTC).

### Por qué esta arquitectura

Se eligió esta arquitectura porque resuelve el caso sin sumar componentes innecesarios.

Las razones principales fueron:

- El volumen del challenge no requiere un motor más grande
- **BigQuery** permite hacer la transformación y deduplicación con SQL simple
- **Cloud Functions** alcanza para una ejecución corta y puntual
- **Cloud Storage** permite conservar evidencia del archivo procesado
- **Cloud Scheduler** deja el proceso listo para correr sin intervención manual.
---

## Objetivo del challenge

El problema a resolver es la carga periódica de un archivo CSV con posibles registros duplicados. A partir de ese archivo se necesita construir una tabla final confiable, manteniendo solo el registro más reciente.

Las key usadas para deduplicar es:

- `id`
- `muestra`
- `resultado`

Además, la solución debía:
- descargar el archivo de forma automática
- guardar una copia del origen para auditoría
- cargar los datos en BigQuery
- permitir reprocesar sin romper la tabla final
- dejar métricas y errores registrados
- poder ejecutarse de forma manual o programada

---

## Flujo de procesamiento

**1. Validación del dataset**  
   - Se verifica que el dataset de BigQuery exista antes de procesar los datos.

**2. Descarga del CSV**  
   - Se descarga el archivo desde la URL.  
   - Se verifica que contenga datos y columnas requeridas.

**3. Subida a GCS**  
   - Se sube el CSV a Cloud Storage en la ruta `raw/YYYY/MM/DD/{ingestion_id}.csv`.

**4. Carga a tabla RAW** (`unificado_raw`)
   - Lectura del CSV  
   - Validación de columnas y campos obligatorios (`id`, `muestra`, `resultado`)
   - Inserción en la tabla `unificado_raw` en BigQuery.

**5. Construcción de la tabla INTERMEDIATE** (`unificado_int`)  
   - Conversión de tipos: `pos` >> INT64, `qual` >> FLOAT64.  
   - Se agregan columnas de metadatos: `ingestion_id` y `source_file`.  
   - Merge con deduplicación usando `ROW_NUMBER()` sobre `id+muestra+resultado`.

**6. Construcción de la tabla FINAL** (`unificado`)  
   - Deduplicación completa: se mantiene la última fila (`fecha_copia DESC`) para cada combinación única `id+muestra+resultado`.  
   - Particionado por `fecha_copia` para optimización de consultas.

**7. Registro de logs** (`unificado_logs`)
   - Se guardan métricas de cada ejecución:
      - Número de filas cargadas a tabla RAW  
      - Total filas en tabla final `unificado`  
      - Filas duplicadas eliminadas  
      - Archivo fuente y ruta en GCS  
      - Estado (`SUCCESS` / `ERROR`) y descripción de errores

### Flujo lógico

```bash
Fuente CSV externa
        |
        v
Cloud Function
        |
        +--> Cloud Storage (archivo de respaldo)
        |
        +--> BigQuery RAW
                 |
                 v
             BigQuery INT
                 |
                 v
            BigQuery FINAL
                 |
                 v
            BigQuery LOGS
```

---

## Modelo de datos por capas

La solución sigue un patrón tipo Medallion. Cada capa cumple una función concreta en el pipeline.

### Capa RAW: `unificado_raw`

Es la tabla donde se carga el archivo fuente casi sin transformación.

Características:

- conserva el dato con la estructura original del CSV
- guarda `fecha_copia` como momento de carga
- está particionada por fecha
- funciona como histórico técnico

Por qué se usó así:

- evita perder evidencia de lo que llegó desde el origen
- permite volver a revisar cargas anteriores
- separa la ingesta de la lógica de negocio

### Capa INT: `unificado_int`

Es la capa intermedia donde se normaliza el dato y se incorporan metadatos.

Características:

- convierte `pos` a `INT64`
- convierte `qual` a `FLOAT64`
- agrega `ingestion_id` y `source_file` para trazabilidad
- se actualiza con `MERGE`

Por qué se usó así:

- permite reprocesar sin generar duplicados lógicos

### Capa FINAL: `unificado`

Es la tabla de consumo final.

Características:

- mantiene una única fila por combinación `id + muestra + resultado`
- elige la fila más reciente por `fecha_copia DESC`

### Capa de auditoría: `unificado_logs`

Registra una fila por ejecución.

Guarda:
- fecha de proceso
- `ingestion_id`
- cantidad de filas cargadas a tabla RAW
- cantidad total de filas en tabla FINAL
- duplicados removidos
- archivo local procesado
- ruta del archivo en GCS
- estado 
- error si ocurrió una falla

Por qué se usó así:

- permite saber qué pasó en cada ejecución
- facilita el soporte
- y deja una evidencia simple para auditar resultados

---

## Estructura de tablas en BigQuery

### Tabla RAW

Tabla: `unificado_raw`
Campos:

- `chrom` STRING
- `pos` STRING
- `id` STRING
- `ref` STRING
- `alt` STRING
- `qual` STRING
- `filter` STRING
- `info` STRING
- `format` STRING
- `muestra` STRING
- `valor` STRING
- `origen` STRING
- `fecha_copia` TIMESTAMP
- `resultado` STRING

### Tabla INT

Tabla: `unificado_int`
Campos:

- `chrom` STRING
- `pos` INT64
- `id` STRING
- `ref` STRING
- `alt` STRING
- `qual` FLOAT64
- `filter` STRING
- `info` STRING
- `format` STRING
- `muestra` STRING
- `valor` STRING
- `origen` STRING
- `fecha_copia` TIMESTAMP
- `resultado` STRING
- `ingestion_id` STRING
- `source_file` STRING

### Tabla FINAL

Tabla: `unificado`

Tiene el mismo esquema que `unificado_int`.

La diferencia no está en el esquema, sino en la regla de construcción: en FINAL solo queda la última fila válida por regla de negocio.

### Tabla LOGS

Tabla: `unificado_logs`
Campos:

- `fecha_proceso` TIMESTAMP
- `ingestion_id` STRING
- `filas_raw` INT64
- `total_filas_tabla_unificado` INT64
- `filas_duplicadas_removidas` INT64
- `ruta_gcs` STRING
- `archivo` STRING
- `status` STRING
- `error` STRING

---

## Cómo se crean los recursos del proyecto

La infraestructura inicial está resuelta con scripts bash. Esto permite repetir el build del entorno sin crear manualmente recursos en consola.

### Configuración base

Archivo: `infra/config.sh`

Define estas variables:

```bash
export PROJECT_ID="pi-data-engineer-challenge"
export DATASET="challenge_data_engineer"
export REGION="us-central1"
export BUCKET_NAME="pi-data-challenge-bucket"
```
### Creación del dataset

Archivo: `infra/dataset_challenge.sh`
- carga la configuración con `source config.sh`,
- crea el dataset con `bq mk`.

Comando:

```bash
bash infra/dataset_challenge.sh
```

Qué hace internamente:

```bash
bq --location=$REGION mk \
  --dataset \
  $PROJECT_ID:$DATASET
```

### Creación del bucket

Archivo: `infra/bucket_raw.sh`

- usa las variables de `config.sh`,
- crea el bucket donde se archivan los archivos procesados.

Comando:

```bash
bash infra/bucket_raw.sh
```

Qué hace internamente:

```bash
gsutil mb -p $PROJECT_ID -l $REGION gs://$BUCKET_NAME
```

Por qué se usó este enfoque:

- separa el respaldo del archivo fuente del procesamiento,
- deja la evidencia del archivo disponible para revisión,
- y permite reutilizar el bucket para cargas manuales o históricas.

### Creación de tablas con bash + SQL

Archivo: `infra/create_tables.sh`

Este script:

- carga `config.sh`,
- recorre todos los archivos SQL de `infra/tables/`,
- reemplaza `${PROJECT_ID}` y `${DATASET}` con `sed`,
- y ejecuta cada SQL con `bq query`.

Comando:

```bash
bash infra/create_tables.sh
```

Lógica principal:

```bash
for file in tables/*.sql
do
  sed "s/\${PROJECT_ID}/$PROJECT_ID/g; s/\${DATASET}/$DATASET/g" $file | \
  bq query --use_legacy_sql=false
done
```
Los archivos usados son:

- `infra/tables/unificado_raw.sql`
- `infra/tables/unificado_int.sql`
- `infra/tables/unificado.sql`
- `infra/tables/unificado_logs.sql`

Todos crean tablas con `CREATE TABLE IF NOT EXISTS` y particionado por fecha.

---

## Carga histórica inicial con `load_backup.py`

Archivo: `infra/load_backup.py`

Su función es poblar la tabla RAW con un archivo histórico inicial.

Qué hace:

- toma `PROJECT_ID`, `DATASET` y `BUCKET_NAME` desde variables de entorno
- arma la referencia a la tabla RAW
- lee un archivo desde `gs://{BUCKET_NAME}/raw/Unificado.csv`
- y lo carga en BigQuery usando `load_table_from_uri`

Comando típico:

```bash
export PROJECT_ID="pi-data-engineer-challenge"
export DATASET="challenge_data_engineer"
export BUCKET_NAME="pi-data-challenge-bucket"

python infra/load_backup.py
```

**Importante:**

- antes de correrlo, el archivo `Unificado.csv` debe existir en `gs://BUCKET_NAME/raw/Unificado.csv`,
- y la tabla RAW ya debe haber sido creada.
---

## Lógica de la Cloud Function
Cloud Function `main.py`

```text
    ┌─→ download_csv()
         ↓
    ┌─→ upload_to_gcs()
         ↓
    ┌─→ load_to_raw()
         ↓
    ┌─→ transform_to_int()
         ↓ 
    ┌─→ merge_to_final()
         ↓
    ╔═════════════════════════════════════╗
    ║  RAW (Bronze)                       ║
    ║  - Datos crudos STRING                
    ║  - fecha_copia timestamp            ║
    ║  - Partitionado DATE(fecha_copia)   ║
    ╚═════════════════════════════════════╝
         ↓
    ┌─→ build_intermediate()
            ↓   
    ╔═════════════════════════════════════╗
    ║  INT (Silver)                       ║
    ║  - Tipos convertidos (INT64,FLOAT64)║
    ║  - ingestion_id, source_file        ║
    ║  - Particionado DATE(fecha_copia)   ║
    ╚═════════════════════════════════════╝
         ↓
    ┌─→ build_final()
            ↓
    ╔═════════════════════════════════════╗
    ║  FINAL (Gold)                       ║
    ║  - Deduplicados (últimos por fecha) ║
    ║  - Metadatos preservados            ║
    ║  - Ready para análisis              ║
    ╚═════════════════════════════════════╝
         ↓  
    ┌─→ insert_log()
            ↓
    ╔═════════════════════════════════════╗ 
    ║  LOGS                               ║
    ║  - Auditoría del proceso            ║
    ║  - Métricas: raw→final→duplicados   ║
    ╚═════════════════════════════════════╝
```

## Validación del dataset antes de procesar

Antes de iniciar la carga, la función verifica que el dataset exista. Para evitar que el pipeline siga corriendo si la base no está lista.

### Descarga del CSV

La función descarga el archivo con `requests.get(..., timeout=30)`.

Por qué se usó así:

- evita ejecuciones colgadas por problemas de red
- y obliga a fallar en forma controlada si la fuente no responde

### Lectura del CSV como `STRING`

El archivo se lee con Pandas usando `dtype=str`. Para evitar errores de inferencia de tipos entre Pandas, PyArrow y BigQuery.

### Validación de columnas y campos obligatorios

Antes de cargar a RAW se valida que estén las columnas esperadas, que el archivo no venga vacío y que las key `id`, `muestra` y `resultado` tengan valor

### Archivo en Cloud Storage antes de procesar

El CSV se sube a GCS en una ruta con fecha y `ingestion_id`.

Ruta:

```text
raw/YYYY/MM/DD/{ingestion_id}.csv
```

Por qué se usó así:

- deja respaldo del archivo realmente procesado
- y ordena el almacenamiento por fecha

### Uso de `MERGE` en la tabla INT

La tabla intermedia no se reconstruye con `CREATE OR REPLACE`. Se actualiza con `MERGE`. Hace más clara la lógica de actualización por clave, soporta reproceso sin duplicar filas lógicas

Además, el `MERGE` usa `ROW_NUMBER()` para quedarse con una sola fila por `id`, `muestra` y `resultado` antes de aplicar el cruce. Eso evita el error clásico de BigQuery donde varias filas fuente matchean con la misma fila destino.

### Uso de `CREATE OR REPLACE` en FINAL

La tabla FINAL sí se reconstruye completa en cada corrida.

Por qué acá sí conviene `CREATE OR REPLACE`:

- la tabla final representa un estado consolidado
- Una reconstrucción completa deja el resultado final fácil de entender y verificar

La lógica final conserva la fila más reciente por clave usando:

- `PARTITION BY id, muestra, resultado`
- `ORDER BY fecha_copia DESC`
- `ROW_NUMBER() = 1`

### Uso de `ingestion_id`

Cada ejecución genera un **UUID**. Esto permite rastrear una corrida puntual, vincula tablas y logs y mejora la auditoría técnica.

### Registro de logs operativos

Al final de cada corrida, exitosa o fallida, se inserta un registro en `unificado_logs`.

![Tabla unificado_logs](img/unificado_log-pi-data-challenge.png)

---

## Estructura modular del código fuente

El código está organizado en 4 módulos independientes para facilitar mantenimiento, testeo y revisión:

### 1. `env_config.py` - Configuración centralizada
Contiene todas las variables de entorno y constantes del proyecto.

**Propósito:**
- Single source of truth para PROJECT_ID, DATASET, credenciales, nombres de tablas
- Evita hardcoding de valores en toda la base de código

**Contenido:**
- `PROJECT_ID` - Proyecto de GCP (desde env)
- `DATASET` - Dataset de BigQuery (desde env)
- `BUCKET_NAME` - Bucket de Cloud Storage (desde env)
- `SOURCE_CSV_URL` - URL del archivo a descargar (desde env)
- `RAW_TABLE`, `INT_TABLE`, `FINAL_TABLE`, `LOGS_TABLE` - Referencias de tablas
- `EXPECTED_COLUMNS` - Schema esperado del CSV para validación

**Por qué se usa así:**
- Cambios de configuración sin tocar lógica de código
- Facilita deploy en diferentes entornos (dev, staging, prod)
- Centraliza todo lo que debe venir de variables de entorno

### 2. `queries.py` - Queries de BigQuery modularizadas
Contiene funciones que generan queries SQL parametrizadas.

**Funciones:**
- `get_create_int_table_query(int_table)` - Genera CREATE TABLE para INT
- `get_merge_int_table_query(int_table, raw_table, ingestion_id, source_file)` - Genera MERGE para INT
- `get_create_final_table_query(final_table, int_table)` - Genera CREATE OR REPLACE para FINAL

**Por qué se usa así:**
- SQL separado del código Python
- Queries parametrizadas => reutilizable en contextos diferentes
- Fácil de revisar, versionar y testear SQL sin ejecutar
- Documentación clara del propósito de cada query

### 3. `data_processing.py` - Lógica medallion architecture
Contiene las funciones que ejecutan la transformación de datos según el patrón medallion.

**Funciones:**
- `load_to_raw(file_path, ingestion_id)` - Cargf CSV a tabla RAW con validaciones
- `build_intermediate(ingestion_id, source_file)` - Crea/actualiza tabla INT con tipos y metadatos
- `build_final()` - Genera tabla FINAL deduplicada

**Por qué se usa así:**
- Agrupa la lógica de transformación en un lugar
- Usa `env_config` para tablas y `queries` para SQL
- Cada función es una etapa clara del medallion: RAW >> INT >> FINAL
- Fácil de testear con datos mock

### 4. `main.py` - Orquestador principal
Contiene la lógica de coordinación del pipeline: validaciones, descarga, y encadenamiento de funciones.

**Funciones principales:**
- `validate_runtime_config()` - Verifica que env vars requeridas existan
- `validate_dataset()` - Verifica que dataset de BigQuery exista
- `download_csv()` - Descarga el archivo desde URL
- `upload_to_gcs(file_path, ingestion_id)` - Sube archivo a Cloud Storage
- `insert_log(...)` - Registra logs de auditoría
- `main(request=None)` - Orquesta todo el flujo, point of entry para Cloud Function

**Responsabilidad:**
- Coordinación del flujo: descarga >> validación >> transformación >> logging
- Manejo de errores centralizadocon try/except
- Retorna status HTTP y JSON con métricas

**Por qué se usa así:**
- Separación de responsabilidades: config, queries, data processing, orquestación
- `main.py` es legible: está claro el flujo paso por paso
- Facilita debugging: si algo falla, está claro en qué etapa
- Preparado para extender: agregar validaciones, loggers, métricas, alertas

### Flujo de dependencias

```text
main.py
  │
  ├─→ import env_config
  │    (PROJECT_ID, DATASET, BUCKET_NAME, etc)
  │
  ├─→ import data_processing
  │    (load_to_raw, build_intermediate, build_final)
  │
  └─→ data_processing.py
       │
       ├─→ import env_config
       │    (RAW_TABLE, INT_TABLE, FINAL_TABLE)
       │
       └─→ import queries
            (get_create_int_table_query, get_merge_int_table_query, get_create_final_table_query)
```

**Ventajas de esta estructura:**
- **Bajo acoplamiento:** Cada módulo tiene una responsabilidad clara
- **Alta cohesión:** Datos relacionados viven en el mismo lugar  
- **Testeable:** Se pueden mockear env vars, queries, componentes de forma independiente
- **Profesional:** Refleja arquitectura de software enterprise-grade

---

## Deploy paso a paso

### Requisitos previos

Antes del deploy:

- autenticarse en GCP
- seleccionar el proyecto
- tener permisos sobre BigQuery, Cloud Storage, Cloud Functions y Scheduler
- y contar con `gcloud`, `bq` y `gsutil` disponibles

Ejemplo:

```bash
gcloud auth login
gcloud config set project pi-data-engineer-challenge
```

### Configurar variables

```bash
source infra/config.sh
```

### Crear dataset

```bash
bash infra/dataset_challenge.sh
```

### Crear bucket

```bash
bash infra/bucket_raw.sh
```
![Bucket en GCP](img/bucket-pi-data-challenge.png)

### Crear tablas en BigQuery

```bash
bash infra/create_tables.sh
```
![BigQuery en GCP](img/bigquerytables-pi-data-challenge.png)

### Cargar histórico inicial si hace falta

Primero subir el archivo histórico a GCS:

```bash
gsutil cp Unificado.csv gs://$BUCKET_NAME/raw/Unificado.csv
```

Luego ejecutar:

```bash
python infra/load_backup.py
```

### Deploy de la Cloud Function

```bash
export PROJECT_ID="pi-data-engineer-challenge"
export DATASET="challenge_data_engineer"
export BUCKET_NAME="pi-data-challenge-bucket"
export REGION="us-central1"
export SOURCE_CSV_URL="https://storagechallengede.blob.core.windows.net/challenge/nuevas_filas%201.csv?sp=r&st=2026-02-04T14:26:43Z&se=2026-12-31T22:41:43Z&sv=2024-11-04&sr=b&sig=9%2Fnsh4qSw6E6fDkdEx8QLBH7kKlC4szAv2Z%2F%2BmLLF4A%3D"
```
```bash
gcloud functions deploy pi-data-challenge-pipeline \
  --gen2 \
  --runtime python311 \
  --region "$REGION" \
  --source=src/ \
  --entry-point main \
  --trigger-http \
  --allow-unauthenticated \
  --memory=512Mi \
  --set-env-vars PROJECT_ID=$PROJECT_ID,DATASET=$DATASET,BUCKET_NAME=$BUCKET_NAME,SOURCE_CSV_URL="$SOURCE_CSV_URL"
```
![Cloud Function en GCP](img/cloudfunction-pi-data-challenge.png)

### Crear el Scheduler

```bash
bash infra/scheduler.sh
```

Este script crea un job HTTP:

```text
0 5 * * 1
```

Eso significa:

- minuto 0,
- hora 5,
- todos los lunes,
- en UTC.

![Cloud Scheduler en GCP](img/cloudscheduler-pi-data-challenge.png)

---

## Ejecución manual y operación

### Ejecutar la Cloud Function manualmente
```bash
curl -X POST "https://pi-data-challenge-pipeline-724141376439.us-central1.run.app" \
-H "Authorization: bearer $(gcloud auth print-identity-token)" \
-H "Content-Type: application/json" \
-d '{
  "name": "Developer"
}'
```
```bash
TRIGGER_URL="https://us-central1-pi-data-engineer-challenge.cloudfunctions.net/pi-data-challenge-pipeline"

curl -X POST "$TRIGGER_URL" \
  -H "Authorization: bearer $(gcloud auth print-identity-token)" \
  -H "Content-Type: application/json" \
  -d '{}'
```
### Ejecutar el Cloud Scheduler manualmente

```bash
gcloud scheduler jobs run pi-data-challenge-weekly \
  --location=$REGION \
  --project=$PROJECT_ID
```

### Ver logs de la Cloud Function

```bash
gcloud functions logs read pi-data-challenge-pipeline \
  --limit 50 \
  --region $REGION \
  --project $PROJECT_ID \
  --follow
```
---

## Consultas útiles de control

#### Ver cantidad de registros finales

```sql
SELECT 
  COUNT(*) AS total_records,
  COUNT(DISTINCT CONCAT(id, muestra, resultado)) AS unique_keys,
  MAX(fecha_copia) AS latest_update
FROM `pi-data-engineer-challenge.challenge_data_engineer.unificado`;
```

#### Ver últimas ejecuciones

```sql
SELECT 
  fecha_proceso,
  ingestion_id,
  filas_raw,
  total_filas_tabla_unificado,
  filas_duplicadas_removidas,
  status,
  error
FROM `pi-data-engineer-challenge.challenge_data_engineer.unificado_logs`
ORDER BY fecha_proceso DESC
LIMIT 10;
```

#### Verificar duplicados en FINAL

```sql
SELECT 
  id,
  muestra,
  resultado,
  COUNT(*) AS cant
FROM `pi-data-engineer-challenge.challenge_data_engineer.unificado`
GROUP BY 1, 2, 3
HAVING cant > 1;
```
---

## Límites actuales y mejoras posibles

Como informe técnico, también es importante dejar claro qué podría mejorarse.

Mejoras posibles:

- mover la URL fuente a una variable de entorno o Secret Manager
- usar `SAFE_CAST` si se quiere tolerar datos defectuosos en vez de cortar el proceso
- agregar alertas automáticas ante error
- usar autenticación más restrictiva en la función si el entorno pasa a producción formal

---

## Conclusión

La solución implementada cumple el objetivo del challenge con una arquitectura simple, trazable y mantenible sobre Google Cloud Platform.

Se eligió una combinación de:

- **Cloud Functions** para la ejecución
- **BigQuery** para el almacenamiento y la transformación
- **Cloud Storage** para el respaldo del archivo csv
- **Cloud Scheduler** para la automatización
- y **scripts bash** para dejar el entorno reproducible

El pipeline es idempotente, auditable y deja evidencia clara de cada ejecución. Además, la lógica de deduplicación es transparente y fácil de verificar.
- con capas bien definidas
- con control de calidad
- con deduplicación clara
- con soporte para reproceso
- y con evidencia de cada ejecución
