# evidence

Logs y resultados de ejecución de cada entrega. Los notebooks de exploración, con salidas guardadas, están en [`../notebooks/`](../notebooks/).

## Segunda entrega (`e2/`)

Los genera el pipeline en cada corrida (`python -m src.pipeline run`); la evidencia oficial es la de la corrida en Google Colab, que el notebook descarga como zip.

| Archivo | Contenido |
|---|---|
| `run.log` | Log completo de la corrida: comando, entorno (Python, Java, PySpark), inicio, fin y resumen de cada etapa |
| `cql_results.txt` | Las dos consultas CQL con sus parámetros y filas devueltas, en tabla |
| `pytest.txt`, `pytest.xml` | Salida de las pruebas (texto y JUnit), generada por el notebook |
| `<etapa>.json` | `status` (`ok`, `pending`, `failed`), `duration_s`, `error`, `inputs` y `outputs` (ruta, filas, archivos, bytes) y `extra` con las métricas de la etapa |

`extra` de `bronze_batch.json`, por fuente:

| Campo | Significado |
|---|---|
| `landing_rows`, `bronze_rows`, `quarantine_rows`, `exact_duplicates` | Conteos; siempre `landing = bronze + quarantine + duplicados` |
| `quarantine_by_reason` | Filas en quarantine por motivo (`malformed_row`, `missing:<col>`, `cast_error:<cols>`, `duplicate_key`) |
| `partitions` | Particiones presentes en Bronze |
| `schema`, `sample` | Esquema escrito y 3 filas de muestra |

`extra` de `bronze_stream.json`:

| Campo | Significado |
|---|---|
| `files`, `micro_batches`, `watermark`, `max_files_per_trigger` | Configuración y lotes procesados en esta corrida |
| `input_rows`, `written_rows`, `bronze_rows` | Leídos y escritos en esta corrida; total en Bronze |
| `duplicates_dropped`, `dropped_by_watermark` | Descartados por `event_id` repetido y por llegar después del watermark |
| `late_arrivals`, `late_arrivals_pct`, `max_lateness` | Eventos aceptados con `event_ts` menor al máximo de micro-lotes anteriores |
| `schema_versions`, `cast_errors`, `corrupt_rows`, `null_event_id`, `null_event_ts` | Perfil de Bronze |
| `partitions`, `files_written`, `sample` | Fechas, archivos Parquet y 3 eventos de muestra |

`extra` de `silver.json`:

| Campo | Significado |
|---|---|
| `bronze_rows`, `silver_rows`, `rejected_rows`, `flagged_rows` | Conteos; siempre `bronze = silver + rejected` |
| `rules` | Eventos que fallan cada regla (una fila puede fallar varias) |
| `unit_missing_with_value` | Eventos con `value` sin `unit`: detectados y resueltos por inferencia |
| `flags` | `cost_anomaly`, `value_missing`, `unit_inferred`, `before_resource_created`, `unknown_schema_version` |
| `features` | Filas no nulas y suma de `requests`, `cpu_hours`, `storage_gb_hours`, `genai_tokens`, `carbon_kg` |
| `join_unmatched`, `masters` | Eventos sin organización o recurso; filas e `ingest_date` de cada maestro |
| `example` | Eventos del ejemplo `EVIDENCE_EXAMPLE` (por defecto `org_cvs4f8cg, 2025-08-17, networking`) y sus totales |
| `quarantine_samples`, `flag_samples` | Hasta 5 filas por motivo de rechazo, de `flagged` y por flag |

`extra` de `gold.json`:

| Campo | Significado |
|---|---|
| `rows`, `orgs`, `series`, `days_per_series` | Tamaño del mart y de las series (org, servicio) usadas por el MAD |
| `reconciliation` | Costo, requests y eventos en Silver y en Gold; la etapa falla si no coinciden |
| `anomalies`, `top_anomalies` | Días anómalos (altos, bajos, umbral, series sin referencia) y los 10 de mayor \|z\| |
| `example` | Fila del mart para `EVIDENCE_EXAMPLE` |
| `processed_at`, `sample` | Momento de la corrida y 3 filas de muestra |

`extra` de `serving.json`:

| Campo | Significado |
|---|---|
| `cassandra_version`, `keyspace` | Versión del nodo y keyspace usado |
| `tables` | Por tabla: filas de origen, filas en Cassandra (deben coincidir) y segundos de carga |
| `latest_usage_date`, `window_days` | Fecha más reciente del mart (referencia de "últimos 14 días") y tamaño de la ventana |
| `queries` | Por consulta: nombre, CQL, parámetros, filas devueltas y duración |
