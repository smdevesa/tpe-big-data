# config

Configuración externalizada por variables de entorno, leídas en [`src/config.py`](../src/config.py). Todas tienen un valor por defecto relativo al repositorio, así que en Google Colab no hay que definir nada. [`.env.example`](.env.example) lista las variables disponibles.

Cassandra corre local dentro del runtime (sin usuario ni token): el repositorio no contiene credenciales.

| Variable | Default | Uso |
|---|---|---|
| `DATALAKE_DIR`, `LANDING_DIR`, `EVIDENCE_DIR` | Relativos al repositorio | Zonas del Data Lake y evidencia |
| `SPARK_MASTER`, `SPARK_SHUFFLE_PARTITIONS` | `local[*]`, `8` | Spark |
| `STREAM_WATERMARK`, `STREAM_MAX_FILES_PER_TRIGGER` | `61 days`, `10` | Streaming de eventos |
| `CASSANDRA_HOST`, `CASSANDRA_KEYSPACE`, `CASSANDRA_VERSION` | `127.0.0.1`, `cloud_analytics`, `5.0.4` | Serving |
| `EVIDENCE_EXAMPLE` | `org_cvs4f8cg,2025-08-17,networking` | Ejemplo `org_id,fecha,servicio` que muestran la evidencia y las consultas |
| `LOG_LEVEL` | `INFO` | Nivel de log |
