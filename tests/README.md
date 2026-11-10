# tests

Pruebas de lógica de negocio (casts, reglas de calidad, deduplicación, features, agregaciones, anomalías) sobre DataFrames chicos construidos en cada test, con una SparkSession compartida (`conftest.py`). No se prueban detalles de infraestructura.

Se ejecutan desde la raíz del repositorio (en Colab, lo hace el notebook):

```bash
python -m pytest -q tests
```

En Colab, la salida queda en `evidence/e2/pytest.txt` y `evidence/e2/pytest.xml` (JUnit).

| Archivo | Qué verifica |
|---|---|
| `test_bronze_batch.py` | Tipos y linaje (`source_file`, `ingest_ts`, `ingest_date`); nulos opcionales; quarantine por cast fallido, clave o `month` nulos y fila malformada; descarte de duplicados exactos; resolución determinística de claves repetidas; conservación `landing = bronze + quarantine + duplicados` |
| `test_bronze_stream.py` | Cast de `value` con texto original; `timestamp` inválido; v1 sin `carbon_kg`/`genai_tokens` queda nulo (no 0); conteo de llegadas fuera de orden; con una query de streaming real sobre archivos temporales: descarte y conteo por watermark con delay corto, aceptación con delay amplio, dedupe por `event_id` sin colapsar eventos sin id, re-ejecución con checkpoint sin reprocesar |
| `test_silver.py` | Cada regla rechaza con su motivo; varios motivos en una fila; duplicado conserva la primera copia; `unit` inferida y métrica desconocida rechazada; umbral de `cost_anomaly` (−0,02 sí, −0,01 no) con copia `flagged`; `value` nulo sin imputar 0; features solo donde aplica la métrica; v1 sin features de v2; `before_resource_created`; enriquecimiento y organización desconocida; foto vigente; ejemplo de E1 (6 eventos, 4,8360 USD, 376 requests) |

Localmente se necesita Java 17 o 21 (`JAVA_HOME`) y Python 3.13 con [`requirements.txt`](../requirements.txt).
