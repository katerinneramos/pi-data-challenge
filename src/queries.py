"""
Queries SQL moduladas para el pipeline medallion.
"""


def get_create_int_table_query(int_table):
    """
    Genera la query para crear la tabla INT si no existe.
    Cambios de tipos: pos INT64, qual FLOAT64
    """
    return f"""
    CREATE TABLE IF NOT EXISTS `{int_table}` (
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
    PARTITION BY DATE(fecha_copia)
    """


def get_merge_int_table_query(int_table, raw_table, ingestion_id, source_file):
    """
    Genera la query para hacer MERGE a la tabla INT.
    - Deduplicación por id + muestra + resultado
    - Mantiene el último registro (fecha_copia DESC)
    - Agrega metadatos: ingestion_id, source_file
    """
    return f"""
    MERGE `{int_table}` AS int
    USING (
        SELECT
            chrom,
            pos,
            id,
            ref,
            alt,
            qual,
            filter,
            info,
            format,
            muestra,
            valor,
            origen,
            fecha_copia,
            resultado,
            ingestion_id,
            source_file
        FROM (
            SELECT 
                chrom,
                CAST(pos AS INT64) AS pos,
                id,
                ref,
                alt,
                CAST(qual AS FLOAT64) AS qual,
                filter,
                info,
                format,
                muestra,
                valor,
                origen,
                fecha_copia,
                resultado,
                '{ingestion_id}' AS ingestion_id,
                '{source_file}' AS source_file,
                ROW_NUMBER() OVER (PARTITION BY id, muestra, resultado ORDER BY fecha_copia DESC) AS rownumber
            FROM `{raw_table}`
        )
        WHERE rownumber = 1
    ) AS raw
    ON int.id = raw.id
       AND int.muestra = raw.muestra
       AND int.resultado = raw.resultado
    
    WHEN MATCHED THEN 
    UPDATE SET 
        chrom = raw.chrom,
        pos = raw.pos,
        ref = raw.ref,
        alt = raw.alt,
        qual = raw.qual,
        filter = raw.filter,
        info = raw.info,
        format = raw.format,
        valor = raw.valor,
        origen = raw.origen,
        fecha_copia = raw.fecha_copia,
        ingestion_id = raw.ingestion_id,
        source_file = raw.source_file
    
    WHEN NOT MATCHED THEN 
    INSERT (
        chrom, pos, id, ref, alt, qual, filter, info, format,
        muestra, valor, origen, fecha_copia, resultado,
        ingestion_id, source_file
    )
    VALUES (
        raw.chrom, raw.pos, raw.id, raw.ref, raw.alt, raw.qual, raw.filter, raw.info, raw.format,
        raw.muestra, raw.valor, raw.origen, raw.fecha_copia, raw.resultado,
        raw.ingestion_id, raw.source_file
    )
    """


def get_create_final_table_query(final_table, int_table):
    """
    Genera la query para crear la tabla FINAL deduplicada.
    - Mantiene un solo registro por id + muestra + resultado
    - Conserva el más reciente (fecha_copia DESC)
    """
    return f"""
    CREATE OR REPLACE TABLE `{final_table}`
    PARTITION BY DATE(fecha_copia) AS
    SELECT
        chrom,
        pos,
        id,
        ref,
        alt,
        qual,
        filter,
        info,
        format,
        muestra,
        valor,
        origen,
        fecha_copia,
        resultado,
        ingestion_id,
        source_file
    FROM (
        SELECT *,
        ROW_NUMBER() OVER(PARTITION BY id, muestra, resultado ORDER BY fecha_copia DESC) rownumber
        FROM `{int_table}`
    )
    WHERE rownumber = 1
    """
