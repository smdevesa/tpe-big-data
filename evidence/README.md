# evidence

Logs y resultados de ejecución de cada entrega. Los notebooks de exploración, con salidas guardadas, están en [`../notebooks/`](../notebooks/).

## Segunda entrega (`e2/`)

Los genera el pipeline en cada corrida (`python -m src.pipeline run`); la evidencia oficial es la de la corrida en Google Colab, que el notebook descarga como zip.

| Archivo | Contenido |
|---|---|
| `run.log` | Log completo de la corrida: comando, entorno (Python, Java, PySpark), inicio, fin y resumen de cada etapa |
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
