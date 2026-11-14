# config

Variables de entorno que lee [`src/config.py`](../src/config.py). Todas tienen valor por defecto; en Colab no hace falta definir ninguna.

| Variable | Default |
|---|---|
| `DATALAKE_DIR`, `LANDING_DIR`, `EVIDENCE_DIR` | Carpetas del repositorio |
| `SPARK_MASTER`, `SPARK_SHUFFLE_PARTITIONS` | `local[*]`, `8` |
| `STREAM_WATERMARK`, `STREAM_MAX_FILES_PER_TRIGGER` | `61 days`, `10` |
| `CASSANDRA_HOST`, `CASSANDRA_KEYSPACE`, `CASSANDRA_VERSION` | `127.0.0.1`, `cloud_analytics`, `5.0.4` |
| `EVIDENCE_EXAMPLE` | `org_cvs4f8cg,2025-08-17,networking` |
| `LOG_LEVEL` | `INFO` |
