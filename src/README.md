# src

Código del pipeline. Se ejecuta con `python -m src.pipeline` desde la raíz del repositorio (ver [`../README.md`](../README.md)).

| Módulo | Responsabilidad |
|---|---|
| `pipeline.py` | Entrypoint: `run [--stage X \| --from X]` y `reset`; ejecuta las etapas en orden y corta ante el primer error |
| `config.py` | Rutas de las zonas y parámetros, sobrescribibles por variables de entorno (ver [`../config/`](../config/)) |
| `logs.py` | Configuración de logging del proyecto (logger `src`, consola y `run.log`) |
| `evidence.py` | Registro de cada etapa: estado (`ok`, `pending`, `failed`), duración, entradas, salidas y métricas en `evidence/e2/<etapa>.json` |
| `spark.py` | `SparkSession` única (UTC, logs de Spark según [`../infra/log4j2.properties`](../infra/log4j2.properties)) |
| `stages/` | Una etapa por zona: `bronze_batch`, `bronze_stream`, `silver`, `gold`, `serving` |
| `bronze/` | Definición de las fuentes batch (`sources.py`) y transformación Landing → Bronze (`transforms.py`) |

## Estado de las etapas

| Etapa | Estado |
|---|---|
| `bronze_batch` | Implementada |
| `bronze_stream`, `silver`, `gold`, `serving` | Pendientes (terminan con estado `pending`) |

Convención: transformaciones como funciones puras sobre DataFrames (testeables sin escribir a disco); las etapas solo leen, llaman a esas funciones, escriben y registran evidencia.
