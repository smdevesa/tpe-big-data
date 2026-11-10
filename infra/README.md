# infra

Scripts y configuración para ejecutar el proyecto desde un entorno limpio. El entorno oficial es Google Colab (ver [`notebooks/e2_pipeline_colab.ipynb`](../notebooks/e2_pipeline_colab.ipynb)).

| Archivo | Qué hace |
|---|---|
| `colab_setup.sh` | Instala OpenJDK 17 y las dependencias de [`requirements.txt`](../requirements.txt) y levanta Cassandra en el runtime de Colab. Idempotente. |
| `cassandra_setup.sh` | Descarga Apache Cassandra (`CASSANDRA_VERSION`, 5.0.4) del archivo oficial de Apache, la inicia single-node y espera el puerto 9042. Si ya responde, no hace nada. |
| `cql/schema.cql` | Keyspace `cloud_analytics` y tablas `usage_by_org_service_day` y `top_services_14d` (`IF NOT EXISTS`); lo ejecuta la etapa `serving` |
| `cql/queries.cql` | Consultas 1 y 2 de la consigna como prepared statements; la etapa `serving` las ejecuta con parámetros de ejemplo |
| `log4j2.properties` | Logs de Spark: nivel WARN, mismo formato que los del proyecto, sin avisos irrelevantes (`NativeCodeLoader`, AQE desactivado en streaming, ajuste de memoria de Parquet). Lo carga [`src/spark.py`](../src/spark.py). |

Localmente, Cassandra se puede levantar con Docker:

```bash
docker run -d --name tpe-cassandra -p 9042:9042 -e MAX_HEAP_SIZE=1G -e HEAP_NEWSIZE=256M cassandra:5.0.4
```
