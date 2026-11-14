# src

Código del pipeline. Se ejecuta con `python -m src.pipeline` desde la raíz del repositorio.

| Módulo | Contenido |
|---|---|
| `pipeline.py` | Comandos `run`, `reset` e `idempotency` |
| `stages/` | Una etapa por zona: `bronze_batch`, `bronze_stream`, `silver`, `gold`, `serving` |
| `bronze/`, `silver/`, `gold/`, `serving/` | Transformaciones de cada etapa |
| `idempotency.py` | Comparación del estado guardado entre dos corridas |
| `config.py`, `logs.py`, `evidence.py`, `spark.py` | Configuración, logs, evidencia por etapa y sesión de Spark |
