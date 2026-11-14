# evidence

Logs y resultados de ejecución de cada entrega. Los notebooks de exploración, con salidas guardadas, están en [`../notebooks/`](../notebooks/).

## Segunda entrega (`e2/`)

La evidencia oficial la genera `python -m src.pipeline idempotency` en Google Colab (el notebook la descarga como zip): `reset` completo, dos corridas del pipeline y la comparación del estado guardado. `python -m src.pipeline run` escribe en `e2/` directamente, para uso diario.

| Evidencia pedida (6.3) | Dónde |
|---|---|
| Logs | `corrida_N/run.log`, `idempotency.log` |
| Rutas, tamaños y conteos | `inputs` y `outputs` de cada `corrida_N/<etapa>.json` (ruta, filas, archivos, bytes) |
| Muestras de quarantine | `quarantine_samples` en `corrida_N/silver.json`; motivos de Bronze en `bronze_batch.json` |
| Capturas de consultas | `corrida_N/cql_results.txt` y `extra.queries` en `serving.json` |
| Idempotencia (6.2) | `idempotency.json` |
| Pruebas | `pytest.txt`, `pytest.xml` |

| Archivo | Contenido |
|---|---|
| `corrida_1/`, `corrida_2/` | Evidencia de cada corrida: un `<etapa>.json` por etapa, `cql_results.txt` y su propio `run.log`. La corrida 1 parte de un estado limpio; la 2 se ejecuta sin `reset` |
| `idempotency.json` | Medidas del estado guardado después de cada corrida (filas, claves distintas y sumas por zona y tabla de Cassandra), diferencias, chequeos y resultado (`ok` / `failed`) |
| `idempotency.log` | `reset` y tabla comparativa de las dos corridas |
| `pytest.txt`, `pytest.xml` | Salida de las pruebas (texto y JUnit), generada por el notebook |

| Archivo dentro de cada corrida | Contenido |
|---|---|
| `run.log` | Log de la corrida: entorno (Python, Java, PySpark), inicio, fin y resumen de cada etapa |
| `<etapa>.json` | `status` (`ok`, `failed`), `duration_s`, `error`, `inputs` y `outputs` (ruta, filas, archivos, bytes) y `extra` con las métricas de la etapa |
| `cql_results.txt` | Las dos consultas CQL con sus parámetros y filas devueltas, en tabla |

Chequeos de `idempotency.json`:

| Chequeo | Significado |
|---|---|
| `same_measures` | Cada medida es igual en las dos corridas |
| `unique_keys` | `event_id` distintos = filas con `event_id` (Bronze y Silver) y claves `(usage_date, org_id, service)` distintas = filas (Gold), en las dos corridas |
| `stream_read_nothing_new` | La corrida 2 no leyó filas del stream (checkpoint) |

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
