# evidence

Logs y resultados de ejecución de cada entrega. Los notebooks de exploración, con salidas guardadas, están en [`../notebooks/`](../notebooks/).

## Segunda entrega (`e2/`)

Los genera el pipeline en cada corrida (`python -m src.pipeline run`); la evidencia oficial es la de la corrida en Google Colab, que el notebook descarga como zip.

| Archivo | Contenido |
|---|---|
| `run.log` | Log completo de la corrida: comando, entorno (Python, Java, PySpark), inicio, fin y resumen de cada etapa |
| `<etapa>.json` | `status` (`ok`, `pending`, `failed`), `duration_s`, `error`, `inputs` y `outputs` (ruta, filas, archivos, bytes) y `extra` con las métricas de la etapa |

`extra` de `bronze_batch.json`, por fuente:

| Campo | Significado |
|---|---|
| `landing_rows`, `bronze_rows`, `quarantine_rows`, `exact_duplicates` | Conteos; siempre `landing = bronze + quarantine + duplicados` |
| `quarantine_by_reason` | Filas en quarantine por motivo (`malformed_row`, `missing:<col>`, `cast_error:<cols>`, `duplicate_key`) |
| `partitions` | Particiones presentes en Bronze |
| `schema`, `sample` | Esquema escrito y 3 filas de muestra |
