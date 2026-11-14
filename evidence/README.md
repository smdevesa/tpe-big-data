# evidence

Evidencia de ejecución de la segunda entrega, generada en Google Colab con `python -m src.pipeline idempotency`.

| Archivo | Contenido |
|---|---|
| `e2/corrida_1/` | Corrida desde cero: un JSON por etapa, `cql_results.txt` y `run.log` |
| `e2/corrida_2/` | Misma corrida sin `reset` |
| `e2/idempotency.json` | Comparación de las dos corridas |
| `e2/pytest.txt` | Salida de las pruebas |

Cada JSON de etapa tiene el estado, las rutas de entrada y salida con filas, archivos y bytes, y las métricas de la etapa (por ejemplo, `quarantine_samples` en `silver.json`).
