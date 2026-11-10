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
| `test_silver.py` | Cada regla rechaza con su motivo; varios motivos en una fila; duplicado conserva la primera copia; `unit` inferida y métrica desconocida rechazada; umbral de `cost_anomaly` (−0,02 sí, −0,01 no) con copia `flagged`; `value` nulo sin imputar 0; features solo donde aplica la métrica; v1 sin features de v2; `schema_version` desconocida marcada; `before_resource_created`; enriquecimiento y organización desconocida; foto vigente; ejemplo de E1 (6 eventos, 4,8360 USD, 376 requests) |
| `test_serving.py` | Ranking de 14 días: un día aporta a las 14 fechas que lo incluyen; la ventana no suma días anteriores; orden por costo con desempate por nombre; costos negativos restan; no se publican fechas posteriores a la última; ranking por organización; `window_start` y `days_with_usage`. La carga y las consultas a Cassandra se verifican en la etapa (conteos y resultados), no con tests |
| `test_gold.py` | Ejemplo de E1 agregado (4,8360 USD, 376 requests, 6 eventos, 1 sin `value`); una fila por organización, servicio y día; métricas sin datos quedan nulas y el 0 informado se conserva; costos negativos sumados y contados; spike alto y día negativo marcados, días normales no; series independientes; sin score si MAD = 0 o la serie es corta; grano único |

Localmente se necesita Java 17 o 21 (`JAVA_HOME`) y Python 3.13 con [`requirements.txt`](../requirements.txt).
