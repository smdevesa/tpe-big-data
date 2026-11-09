# tests

Pruebas de lógica de negocio (casts, reglas de calidad, deduplicación, features, agregaciones, anomalías) sobre DataFrames chicos construidos en cada test, con una SparkSession compartida (`conftest.py`). No se prueban detalles de infraestructura.

Se ejecutan desde la raíz del repositorio (en Colab, lo hace el notebook):

```bash
python -m pytest -q tests
```

| Archivo | Qué verifica |
|---|---|
| `test_bronze_batch.py` | Tipos y linaje (`source_file`, `ingest_ts`, `ingest_date`); nulos opcionales; quarantine por cast fallido, clave o `month` nulos y fila malformada; descarte de duplicados exactos; resolución determinística de claves repetidas; conservación `landing = bronze + quarantine + duplicados` |

Localmente se necesita Java 17 o 21 (`JAVA_HOME`) y Python 3.13 con [`requirements.txt`](../requirements.txt).
