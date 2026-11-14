# tests

Pruebas de lógica de negocio. Se corren desde la raíz del repositorio:

```bash
python -m pytest -q tests
```

| Archivo | Qué prueba |
|---|---|
| `test_bronze_batch.py` | Casts, quarantine y deduplicación de los maestros |
| `test_bronze_stream.py` | Parseo de eventos, watermark, deduplicación y checkpoint con un stream real |
| `test_silver.py` | Reglas de calidad, flags, features y enriquecimiento |
| `test_gold.py` | Agregación diaria y detección de anomalías |
| `test_serving.py` | Ranking de 14 días |
| `test_idempotency.py` | Comparación de dos corridas |
